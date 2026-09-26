"""Synthetic GNU command responses exercise the actual bootstrap I/O boundary."""
import copy
import stat

import pytest

from snow_statistics import real_writer_bootstrap as module

CONTAINER = "snow-real-fixture-bootstrap-jobmanager"
STAMP = "2026-09-19 09:00:00.000000000 +0000"
JAAS_PATHS = ["/flink-state/tmp/jaas-" + str(number) + ".conf" for number in (123, 456)]
RPC_PATHS = ["/flink-state/tmp/flink-rpc-akka00000000-0000-4000-8000-00000000000" + str(number) + ".jar"
             for number in (1, 2)]


def synthetic_bootstrap():
    """Metadata-only fixture for registry integration tests; no real rows/files."""
    directories = [dict(path=path, mode="755", uid=9999, gid=9999)
                   for path in (*module.ROOTS, "/flink-state/tmp", "/flink-state/tmp/jm_fixture",
                                "/flink-state/tmp/jm_fixture/blobStorage")]
    files = [dict(path=path, mode="644", uid=9999, gid=9999, links=1, **resource)
             for paths, resource in ((JAAS_PATHS, module.JAAS), (RPC_PATHS, module.RPC)) for path in paths]
    return dict(schema_version=1, image_resource=dict(module.DIST), files=files, directories=directories)


class BootstrapDocker:
    """Minimal fake filesystem, not a fake precomputed verification result.

    `.command(argv, timeout=)` accepts only the exact permitted commands and
    renders find/stat/sha256sum responses from independently mutable nodes.
    """
    def __init__(self, container=CONTAINER):
        self.container = container
        self.calls = []
        self.nodes = {}
        self.hook = None
        self.output_override = None
        for row in synthetic_bootstrap()["directories"]:
            self.add(row["path"], mode=stat.S_IFDIR | 0o755, links=2, size=4096)
        for row in synthetic_bootstrap()["files"]:
            self.add(row["path"], size=row["bytes"], sha256=row["sha256"])
        self.add(module.DIST["path"], size=module.DIST["bytes"], sha256=module.DIST["sha256"])

    def add(self, path, *, mode=stat.S_IFREG | 0o644, links=1, size=1179, sha256="0" * 64):
        self.nodes[path] = dict(mode=mode, size=size, links=links, uid=9999, gid=9999,
                                inode=len(self.nodes) + 100, device=30, modified=STAMP, changed=STAMP,
                                sha256=sha256)

    def command(self, argv, *, timeout):
        self.calls.append(argv)
        assert argv[:2] == ["exec", self.container] and timeout == 15
        args = argv[2:]
        if self.hook:
            self.hook(args)
        if self.output_override:
            result = self.output_override(args)
            if result is not None:
                return result
        if args == ["find", "-P", *module.ROOTS, "-print0"]:
            paths = [p for p in self.nodes if p != module.DIST["path"]]
            return ("\0".join(paths) + "\0").encode()
        if args[:4] == ["stat", "--printf", module.STAT_FORMAT, "--"] and len(args) == 5:
            row = self.nodes[args[4]]
            fields = [format(row["mode"], "x"), *(str(row[k]) for k in
                      ("size", "links", "uid", "gid", "inode", "device", "modified", "changed"))]
            return "|".join(fields).encode()
        if args[:2] == ["sha256sum", "--"] and len(args) == 3:
            return (self.nodes[args[2]]["sha256"] + "  " + args[2] + "\n").encode()
        raise AssertionError("Forbidden command: " + repr(args))


def test_actual_command_boundary_has_no_body_reads_and_checks_source_twice():
    docker = BootstrapDocker()
    actual = module.verify_bootstrap(docker.command, CONTAINER)
    assert module.validate_bootstrap(actual) == actual
    assert len(actual["files"]) == 4
    assert actual["directories"] == sorted(synthetic_bootstrap()["directories"], key=lambda r: r["path"])
    assert all(call[2] in {"find", "stat", "sha256sum"} for call in docker.calls)
    assert sum(call[2:] == ["sha256sum", "--", module.DIST["path"]] for call in docker.calls) == 2
    assert sum(call[2] == "find" for call in docker.calls) == 2
    assert set(actual) == {"schema_version", "image_resource", "files", "directories"}


@pytest.mark.parametrize("field,value", [
    ("sha256", "f" * 64), ("size", 1180), ("uid", 0), ("gid", 0),
    ("mode", stat.S_IFREG | 0o600), ("links", 2),
    ("mode", stat.S_IFLNK | 0o777), ("mode", stat.S_IFIFO | 0o644),
])
def test_pollution_links_permissions_and_body_changes_fail(field, value):
    docker = BootstrapDocker()
    docker.nodes[JAAS_PATHS[0]][field] = value
    with pytest.raises(ValueError):
        module.verify_bootstrap(docker.command, CONTAINER)


@pytest.mark.parametrize("path", [
    "/checkpoints/chk-1", "/checkpoints/empty-dir", "/flink-state/unregistered.jar",
    "/flink-state/tmp/unknown.jar", "/flink-state/tmp/jm_fixture/jaas-987.conf",
    "/flink-state/tmp/../escape", "/flink-state//tmp/duplicate", "/flink-state/tmp/a\nsecret",
])
def test_unknown_paths_are_rejected_without_reading_their_body(path):
    docker = BootstrapDocker()
    docker.add(path, sha256=module.JAAS["sha256"])
    with pytest.raises(ValueError):
        module.verify_bootstrap(docker.command, CONTAINER)
    assert not any(call[2:] == ["sha256sum", "--", path] for call in docker.calls)


@pytest.mark.parametrize("change", ["missing", "extra", "wrong-kind"])
def test_exact_two_per_jvm_type_is_required(change):
    docker = BootstrapDocker()
    if change == "missing":
        del docker.nodes[RPC_PATHS[0]]
    elif change == "extra":
        docker.add("/flink-state/tmp/jaas-999.conf", sha256=module.JAAS["sha256"])
    else:
        del docker.nodes[RPC_PATHS[0]]
        docker.add("/flink-state/tmp/jaas-999.conf", sha256=module.JAAS["sha256"])
    with pytest.raises(ValueError):
        module.verify_bootstrap(docker.command, CONTAINER)


@pytest.mark.parametrize("field,value", [("sha256", "f" * 64), ("size", 1),
                                          ("mode", stat.S_IFLNK | 0o777), ("links", 2)])
def test_different_distribution_is_refused_before_state_scan(field, value):
    docker = BootstrapDocker()
    docker.nodes[module.DIST["path"]][field] = value
    with pytest.raises(ValueError):
        module.verify_bootstrap(docker.command, CONTAINER)
    assert not any(call[2] == "find" for call in docker.calls)


def test_symlink_parent_is_not_followed_or_accepted():
    docker = BootstrapDocker()
    docker.nodes["/flink-state/tmp"]["mode"] = stat.S_IFLNK | 0o777
    with pytest.raises(ValueError, match="Links"):
        module.verify_bootstrap(docker.command, CONTAINER)


@pytest.mark.parametrize("mutation", ["new-file", "same-inode-rewrite", "source-replace"])
def test_mutation_during_the_actual_probe_fails(mutation):
    docker = BootstrapDocker()
    invocations = 0
    def mutate(args):
        nonlocal invocations
        if args[0] == "find":
            invocations += 1
            if invocations == 2:
                if mutation == "new-file":
                    docker.add("/flink-state/tmp/late-payload")
                elif mutation == "same-inode-rewrite":
                    docker.nodes[JAAS_PATHS[0]]["changed"] = "2026-09-19 09:00:00.000000001 +0000"
                else:
                    docker.nodes[module.DIST["path"]]["inode"] += 1
    docker.hook = mutate
    with pytest.raises(ValueError):
        module.verify_bootstrap(docker.command, CONTAINER)


@pytest.mark.parametrize("kind", ["oversize", "non-ascii", "missing-nul", "duplicate", "bad-stat", "wrong-hash-path"])
def test_malformed_command_output_never_becomes_an_admission(kind):
    docker = BootstrapDocker()
    def output(args):
        if args[0] == "find":
            if kind == "oversize":
                return b"x" * 65537
            if kind == "non-ascii":
                return b"\xff\0"
            if kind == "missing-nul":
                return b"/checkpoints"
            if kind == "duplicate":
                return ("\0".join(["/checkpoints"] * 7) + "\0").encode()
        if kind == "bad-stat" and args[0] == "stat":
            return b"81a4|1|1|9999|9999|2|3"
        if kind == "wrong-hash-path" and args[0] == "sha256sum":
            return (module.DIST["sha256"] + "  /wrong/path\n").encode()
        return None
    docker.output_override = output
    with pytest.raises(ValueError):
        module.verify_bootstrap(docker.command, CONTAINER)


def test_failure_and_untrusted_container_arguments_do_not_run_an_alternate_command():
    docker = BootstrapDocker()
    for name in ("other", CONTAINER + ";id", CONTAINER + "\n", "snow-real-fixture-taskmanager"):
        with pytest.raises(ValueError):
            module.verify_bootstrap(docker.command, name)
    assert docker.calls == []
    def failure(args, **kwargs):
        raise TimeoutError("synthetic timeout")
    with pytest.raises(TimeoutError):
        module.verify_bootstrap(failure, CONTAINER)


@pytest.mark.parametrize("change", ["extra-key", "unknown-file", "missing-parent", "duplicate", "bool", "bad-source"])
def test_registry_metadata_validation_is_independently_strict(change):
    value = copy.deepcopy(synthetic_bootstrap())
    if change == "extra-key":
        value["passed"] = True
    elif change == "unknown-file":
        value["files"][0]["path"] = "/flink-state/tmp/arbitrary.conf"
    elif change == "missing-parent":
        value["directories"] = [r for r in value["directories"] if r["path"] != "/flink-state/tmp/jm_fixture"]
    elif change == "duplicate":
        value["directories"].append(value["directories"][0])
    elif change == "bool":
        value["files"][0]["links"] = True
    else:
        value["image_resource"]["sha256"] = "0" * 64
    with pytest.raises(ValueError):
        module.validate_bootstrap(value)
