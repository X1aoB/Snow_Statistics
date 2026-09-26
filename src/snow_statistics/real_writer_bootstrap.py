"""Recognize only the locked Flink image's four fresh-JVM temporary files.

This is not an epoch admission or an initialization receipt by itself. The caller
must additionally verify the original empty volumes, Docker identities, frozen
image/configuration and absence of Flink jobs. No payload is read or returned.
"""
import re
import stat
from pathlib import PurePosixPath

SOURCE_REVISION = "01e3a6d78d58843d7e67d94bfcbcc45337677d74"
SOURCE_ROOT = "https://github.com/apache/flink/blob/" + SOURCE_REVISION + "/"
OFFICIAL_SOURCES = (
    SOURCE_ROOT + "flink-runtime/src/main/java/org/apache/flink/runtime/security/modules/JaasModule.java",
    SOURCE_ROOT + "flink-runtime/src/main/resources/flink-jaas.conf",
    SOURCE_ROOT + "flink-rpc/flink-rpc-akka-loader/src/main/java/org/apache/flink/runtime/rpc/pekko/PekkoRpcSystemLoader.java",
    SOURCE_ROOT + "flink-rpc/flink-rpc-akka-loader/pom.xml",
)
DIST = dict(path="/opt/flink/lib/flink-dist-1.20.3.jar", bytes=125950125,
            sha256="ffa50ee730d4cfb3bf2269daefd51bcabc30ff4936e770e0255edd0cb0d70a0a")
JAAS = dict(bytes=1179, sha256="45ae064aea6523a5e3cf028276873e5dff2f612281516c39aefe46cb0e0f99c7")
RPC = dict(bytes=21439218, sha256="d4e8ecc4808cec2036e0df82f7d5dc4ad286d4b562276841c496bed047a7769b")
UUID4 = r"[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}"
JAAS_NAME = re.compile(r"jaas-[0-9]{1,20}\.conf")
RPC_NAME = re.compile(r"flink-rpc-akka" + UUID4 + r"\.jar")
ROOTS = ("/checkpoints", "/flink-state")
MAX_ENTRIES = 128
STAT_FORMAT = "%f|%s|%h|%u|%g|%i|%d|%y|%z"


def _keys(value, expected):
    if type(value) is not dict or set(value) != set(expected):
        raise ValueError("Unexpected Flink bootstrap metadata fields")


def _path(value):
    if (type(value) is not str or len(value) > 240 or
            not re.fullmatch(r"/[A-Za-z0-9_./:-]+", value) or
            str(PurePosixPath(value)) != value or
            any(part in {".", ".."} for part in value.split("/"))):
        raise ValueError("Noncanonical Flink bootstrap path")
    if value not in ROOTS and value != "/flink-state/tmp" and not value.startswith("/flink-state/tmp/"):
        raise ValueError("Unexpected path outside the Flink bootstrap tmp scope")
    return value


def validate_bootstrap(value):
    """Validate a recorded metadata projection; never accept it as an I/O probe."""
    _keys(value, ("schema_version", "image_resource", "files", "directories"))
    if type(value["schema_version"]) is not int or value["schema_version"] != 1:
        raise ValueError("Unsupported Flink bootstrap metadata version")
    _keys(value["image_resource"], DIST)
    if value["image_resource"] != DIST or type(value["image_resource"]["bytes"]) is not int:
        raise ValueError("Locked Flink distribution identity changed")
    files, directories = value["files"], value["directories"]
    if type(files) is not list or len(files) != 4 or type(directories) is not list:
        raise ValueError("A fresh JM/TM pair needs exactly four bootstrap files")
    if not 3 <= len(directories) <= MAX_ENTRIES - 4:
        raise ValueError("Unbounded or incomplete bootstrap directory inventory")
    all_paths, directory_paths, kinds = set(), set(), []
    for row in directories:
        _keys(row, ("path", "mode", "uid", "gid"))
        path = _path(row["path"])
        if path in all_paths:
            raise ValueError("Duplicate bootstrap path")
        # The owned fresh volumes are initialized by the locked entrypoint.
        if (row["mode"] != "755" or type(row["uid"]) is not int or type(row["gid"]) is not int or
                row["uid"] != 9999 or row["gid"] != 9999):
            raise ValueError("Unexpected Flink bootstrap directory ownership/mode")
        all_paths.add(path)
        directory_paths.add(path)
    if not {*ROOTS, "/flink-state/tmp"} <= directory_paths:
        raise ValueError("Missing owned Flink bootstrap directories")
    for row in files:
        _keys(row, ("path", "bytes", "mode", "links", "uid", "gid", "sha256"))
        path = _path(row["path"])
        if path in all_paths or str(PurePosixPath(path).parent) != "/flink-state/tmp":
            raise ValueError("Bootstrap files must be unique direct children of tmp")
        if (any(type(row[field]) is not int for field in ("bytes", "links", "uid", "gid")) or
                row["mode"] != "644" or row["links"] != 1 or row["uid"] != 9999 or row["gid"] != 9999):
            raise ValueError("Unexpected Flink bootstrap file ownership/mode/links")
        name = PurePosixPath(path).name
        if JAAS_NAME.fullmatch(name):
            expected, kind = JAAS, "jaas"
        elif RPC_NAME.fullmatch(name):
            expected, kind = RPC, "rpc"
        else:
            raise ValueError("Unrecognized Flink bootstrap file")
        if row["bytes"] != expected["bytes"] or row["sha256"] != expected["sha256"]:
            raise ValueError("Bootstrap file differs from the locked embedded resource")
        kinds.append(kind)
        all_paths.add(path)
    if kinds.count("jaas") != 2 or kinds.count("rpc") != 2:
        raise ValueError("A fresh JM/TM pair needs exactly two JAAS and two RPC files")
    for path in all_paths - set(ROOTS):
        if str(PurePosixPath(path).parent) not in directory_paths:
            raise ValueError("Incomplete bootstrap parent-directory inventory")
    return value


def verify_bootstrap(command, container):
    """Read actual bounded Docker metadata through Docker.command(argv, timeout=).

    No shell, file body, arbitrary path or precomputed success JSON is accepted.
    The second inventory and per-file stat recheck reject concurrent mutation.
    """
    if not isinstance(container, str) or not re.fullmatch(r"snow-real-[a-z][a-z0-9-]{2,23}-jobmanager", container):
        raise ValueError("Expected the exact owned epoch JobManager name")

    def run(arguments, limit=65536):
        result = command(["exec", container, *arguments], timeout=15)
        if type(result) is not bytes or len(result) > limit:
            raise ValueError("Unbounded Flink bootstrap command response")
        try:
            return result.decode("ascii")
        except UnicodeDecodeError as exc:
            raise ValueError("Unexpected non-ASCII bootstrap metadata") from exc

    def metadata(path):
        raw = run(["stat", "--printf", STAT_FORMAT, "--", path], 256)
        parts = raw.split("|")
        timestamp = r"[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{9} [+-][0-9]{4}"
        if (len(parts) != 9 or not re.fullmatch(r"[a-f0-9]+", parts[0]) or
                any(not re.fullmatch(r"[0-9]{1,20}", part) for part in parts[1:7]) or
                any(not re.fullmatch(timestamp, part) for part in parts[7:])):
            raise ValueError("Malformed bootstrap stat result")
        return (int(parts[0], 16), *(int(part) for part in parts[1:7]), *parts[7:])

    def checksum(path):
        raw = run(["sha256sum", "--", path], 512)
        match = re.fullmatch(r"([a-f0-9]{64})  " + re.escape(path) + r"\n", raw)
        if not match:
            raise ValueError("Malformed bootstrap checksum result")
        return match.group(1)

    def distribution():
        before = metadata(DIST["path"])
        if not stat.S_ISREG(before[0]) or before[1] != DIST["bytes"] or before[2] != 1:
            raise ValueError("Locked distribution must be a regular single-link file")
        if checksum(DIST["path"]) != DIST["sha256"] or metadata(DIST["path"]) != before:
            raise ValueError("Locked Flink distribution identity changed")
        return before

    def inventory():
        raw = run(["find", "-P", *ROOTS, "-print0"])
        if not raw.endswith("\0"):
            raise ValueError("Incomplete bootstrap inventory")
        paths = raw[:-1].split("\0")
        if not 7 <= len(paths) <= MAX_ENTRIES or len(set(paths)) != len(paths):
            raise ValueError("Invalid bootstrap inventory cardinality")
        for path in paths:
            _path(path)
        return sorted(paths)

    source_before = distribution()
    paths = inventory()
    files, directories, observed = [], [], {}
    for path in paths:
        before = metadata(path)
        observed[path] = before
        mode, size, links, uid, gid = before[:5]
        row = dict(path=path, mode=format(stat.S_IMODE(mode), "o"), uid=uid, gid=gid)
        if stat.S_ISDIR(mode):
            directories.append(row)
        elif stat.S_ISREG(mode):
            # Reject foreign, nested or unbounded files before asking for a hash.
            name = PurePosixPath(path).name
            allowed = JAAS if JAAS_NAME.fullmatch(name) else RPC if RPC_NAME.fullmatch(name) else None
            if (str(PurePosixPath(path).parent) != "/flink-state/tmp" or allowed is None or
                    size != allowed["bytes"] or links != 1 or uid != 9999 or gid != 9999 or row["mode"] != "644"):
                raise ValueError("Unexpected file in fresh Flink state")
            actual_sha = checksum(path)
            if actual_sha != allowed["sha256"]:
                raise ValueError("Bootstrap file differs from the locked embedded resource")
            files.append(row | dict(bytes=size, links=links, sha256=actual_sha))
        else:
            raise ValueError("Links and special files are forbidden in fresh Flink state")
    if inventory() != paths or any(metadata(path) != observed[path] for path in paths):
        raise ValueError("Flink bootstrap state changed during inspection")
    if distribution() != source_before:
        raise ValueError("Locked Flink distribution changed during inspection")
    return validate_bootstrap(dict(schema_version=1, image_resource=dict(DIST), files=files, directories=directories))
