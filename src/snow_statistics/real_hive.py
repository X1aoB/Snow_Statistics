"""Aggregate-only external Hive registration, with actual catalog lifecycle checks.

This is independent of the frozen production writer. Hive owns catalog metadata;
the existing remote lifecycle owns the original Parquet files and their expiry.
No receipt supplied by a user can substitute for the Catalog adapter's execution.
"""
import ipaddress
import json
import os
import re
import signal
import subprocess
import threading
from contextlib import contextmanager
from datetime import UTC, datetime, time, timedelta
from pathlib import Path

from .io import atomic_write, digest, write_json
from .lifecycle import timestamp
from .publication import canonical, publication_lock
from .real_behavior import HK, real_path
from .real_publication import read_real_release, validate_real_pair

GROUPS = ("daily", "session_daily", "retention", "funnel")
_RUNNER_LOCK = threading.Lock()
_PROCESS_RUNNER = None


@contextmanager
def catalog_runner_context(runner):
    """An explicit, finite process context, including Streamlit script threads.

    Only a trusted live coordinator installs this callable. Environment variables,
    saved receipts and operator JSON never select a backend or grant permission.
    The ordinary CLI retains its local worker when no context is installed.
    """
    global _PROCESS_RUNNER
    if not callable(runner):
        raise ValueError("A live catalog execution adapter is required")
    with _RUNNER_LOCK:
        if _PROCESS_RUNNER is not None:
            raise ValueError("A catalog execution session is already active")
        _PROCESS_RUNNER = runner
    try:
        yield
    finally:
        with _RUNNER_LOCK:
            _PROCESS_RUNNER = None


def _catalog_runner():
    with _RUNNER_LOCK:
        return _PROCESS_RUNNER or run_catalog_worker


def run_catalog_worker(command, *, cwd, timeout, **unused):
    """Give the shell's exact-container EXIT cleanup time to run on interruption."""
    process = subprocess.Popen(command, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               text=True, start_new_session=True)
    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except BaseException:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.communicate(timeout=50)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=10)
        raise
    if process.returncode:
        raise subprocess.CalledProcessError(process.returncode, command, output=stdout, stderr=stderr)
    return subprocess.CompletedProcess(command, process.returncode, stdout, stderr)


def rows_hash(rows):
    return digest(canonical(sorted(rows, key=canonical)))


def plan(release, owner, artifacts, *, now=None):
    current = now or datetime.now(UTC)
    daily, behavior = validate_real_pair(release["daily"], release["behavior"])
    collector = daily["input_snapshot"]["collector"]
    if any(collector[key] != owner[key] for key in ("instance_id", "generation")):
        raise ValueError("Hive release differs from the registered collector generation")
    origin = datetime.combine(datetime.fromisoformat(daily["date_from"]).date(), time(), HK)
    expires = origin + timedelta(days=90)
    if origin > current or expires <= current or timestamp(release["expires_at"]) != expires:
        raise ValueError("Hive cannot copy expired aggregates or renew their original lifetime")
    owner_hash, release_hash = digest(canonical(owner)), digest(canonical(release))
    pair_hash = digest(canonical({"daily": release["daily"], "behavior": release["behavior"]}))
    result = {}
    for group in GROUPS:
        location = daily["output"] + "/ads_daily" if group == "daily" else behavior["output"] + "/" + group
        real_path(location, "warehouse")
        if not location.startswith(owner["roots"]["warehouse"] + "/"):
            raise ValueError("Hive table location escaped the registered real lane")
        artifact = artifacts.get(location)
        if (not artifact or artifact["kind"] != "aggregate" or
                timestamp(artifact["original_min_accepted_at"]) != origin or timestamp(artifact["expires_at"]) != expires):
            raise ValueError("Hive must reference an already registered aggregate with the same expiry")
        values = release["daily"]["daily"] if group == "daily" else release["behavior"]["aggregates"][group]
        table = "snow_real.catalog_" + owner_hash[:32] + "_" + release_hash[:32] + "_" + group
        result[table] = dict(group=group, location=location, owner_sha256=owner_hash, collector=collector,
                             release_sha256=release_hash, pair_sha256=pair_hash, original_at=origin.isoformat(),
                             expires_at=expires.isoformat(), expected_rows=len(values), expected_rows_sha256=rows_hash(values))
    return result


class HiveRegistry:
    """Metadata-only intent is durable BEFORE any catalog mutation; retries keep it."""
    def __init__(self, manager):
        self.manager = manager
        self.path = manager.directory / "hive-catalog.json"

    def read(self):
        owner, _ = self.manager._read()
        if not self.path.exists():
            return dict(schema_version=1, owner_sha256=digest(canonical(owner)), tables={})
        if self.path.resolve() != self.path.absolute() or self.path.stat().st_size > 8 * 1024**2:
            raise ValueError("Hive metadata must be bounded and cannot traverse links")
        data = json.loads(self.path.read_bytes())
        if (set(data) != {"schema_version", "owner_sha256", "tables"} or data["schema_version"] != 1 or
                data["owner_sha256"] != digest(canonical(owner)) or not isinstance(data["tables"], dict) or len(data["tables"]) > 4000):
            raise ValueError("Invalid or unbounded Hive ownership registry")
        # Shared Python 3.8 contract is also used inside the pinned Spark driver.
        from .real_hive_contract import validate_descriptor
        for table, entry in data["tables"].items():
            if set(entry) != {"descriptor", "verified"} or type(entry["verified"]) is not bool:
                raise ValueError("Invalid Hive registration state")
            validate_descriptor(table, entry["descriptor"])
            if entry["descriptor"]["owner_sha256"] != data["owner_sha256"]:
                raise ValueError("Hive table changed its registered owner")
            descriptor = entry["descriptor"]
            if (not descriptor["location"].startswith(owner["roots"]["warehouse"] + "/") or
                    any(descriptor["collector"][key] != owner[key] for key in ("instance_id", "generation"))):
                raise ValueError("Hive descriptor escaped the registered collector or warehouse")
        return data

    def reserve(self, release, *, now=None):
        owner, state = self.manager._read()
        descriptors = plan(release, owner, state["artifacts"], now=now)
        with publication_lock(self.manager.directory):
            if self.manager.journal.exists():
                raise ValueError("Finish failed remote cleanup before reserving Hive tables")
            data = self.read()
            for table, descriptor in descriptors.items():
                if table in data["tables"] and data["tables"][table]["descriptor"] != descriptor:
                    raise ValueError("A Hive retry cannot change its immutable registration")
                data["tables"].setdefault(table, dict(descriptor=descriptor, verified=False))
            write_json(self.path, data)
        # A crash between these writes is recoverable by reserve() with the same
        # managed release. No catalog command runs until all reservations match.
        for table, descriptor in descriptors.items():
            self.manager.register_backend("hive", table, "aggregate", descriptor["original_at"], now=now)
        return descriptors

    def verified(self, descriptors):
        with publication_lock(self.manager.directory):
            data = self.read()
            for table, value in descriptors.items():
                if data["tables"][table]["descriptor"] != value:
                    raise ValueError("Hive ownership changed during registration")
                data["tables"][table]["verified"] = True
            write_json(self.path, data)


class HiveRetention:
    def __init__(self, registry, catalog):
        self.registry, self.catalog = registry, catalog

    def purge_and_verify(self, resources, now):
        data = self.registry.read()
        if set(resources) != set(data["tables"]):
            raise ValueError("Hive cleanup requires every exact registered catalog resource")
        for table, entry in resources.items():
            descriptor = data["tables"][table]["descriptor"]
            if (entry["kind"] != "aggregate" or timestamp(entry["original_min_accepted_at"]) != timestamp(descriptor["original_at"]) or
                    timestamp(entry["expires_at"]) != timestamp(descriptor["expires_at"])):
                raise ValueError("Hive catalog and data lifetime differ")
        result = self.catalog.execute("cleanup", data["tables"], now)
        if set(result["tables"]) != set(resources):
            raise ValueError("Hive readback omitted a registered table")
        live = []
        for table, row in result["tables"].items():
            entry = data["tables"][table]
            expired = timestamp(entry["descriptor"]["expires_at"]) <= now
            if expired and row["state"] != "absent" or not expired and row["state"] not in {"present", "absent"}:
                raise ValueError("Expired Hive catalog reference remains")
            if row["state"] == "present":
                live.append(entry["descriptor"]["expires_at"])
            elif not expired and entry["verified"]:
                raise ValueError("Previously verified live Hive table disappeared")
        return dict(backend="hive", resources_sha256=digest(canonical(resources)), checked_at=now.isoformat(),
                    evidence_sha256=digest(canonical(result)), remaining_expired=0,
                    live_records=len(live), next_expiry=min(live, key=timestamp) if live else None,
                    verification="external_catalog_metadata_only", hdfs_payload_deleted=False)


def register_release(directory, registry, catalog, cleanup, *, now=None, verify_only=False):
    """cleanup is an actual in-process remote lifecycle call, never a flag/file."""
    current = now or datetime.now(UTC)
    release = read_real_release(directory, current)
    if verify_only:
        owner, state = registry.manager._read()
        descriptors = plan(release, owner, state["artifacts"], now=current)
        registered = registry.read()["tables"]
        if any(key not in registered or registered[key]["descriptor"] != value or not registered[key]["verified"] for key, value in descriptors.items()):
            raise ValueError("Verify requires an already verified immutable Hive registration")
    else:
        descriptors = registry.reserve(release, now=current)
    cleanup()  # ODS + all initialized backends, including this Hive scope.
    if registry.manager.journal.exists():
        raise ValueError("Remote cleanup is incomplete")
    result = catalog.execute("verify" if verify_only else "register", {key: dict(descriptor=value, verified=verify_only) for key, value in descriptors.items()}, current)
    if set(result["tables"]) != set(descriptors):
        raise ValueError("Incomplete Hive aggregate readback")
    for table, row in result["tables"].items():
        expected = descriptors[table]
        if (row.get("state") != "present" or row.get("rows") != expected["expected_rows"] or
                row.get("rows_sha256") != expected["expected_rows_sha256"]):
            raise ValueError("Hive aggregate values differ from the accepted release")
    registry.verified(descriptors)
    # Check again before publishing success: expiry/backend changes during a
    # long catalog operation cannot be acknowledged as a fresh read permit.
    cleanup()
    return result


class SparkCatalog:
    """Execute the fixed, hash-bound local[1] driver; not arbitrary SQL or shell."""
    def __init__(self, root, registry, metastore_host, *, runner=None):
        self.root, self.registry, self.runner = Path(root).absolute(), registry, runner or _catalog_runner()
        address = ipaddress.ip_address(metastore_host)
        if address.version != 4 or not address.is_private or address.is_loopback:
            raise ValueError("Use the configured private lab control IP")
        self.metastore_uri = "thrift://" + metastore_host + ":9083"

    def execute(self, action, tables, now):
        if action not in {"cleanup", "register", "verify"}:
            raise ValueError("Unknown catalog operation")
        request = dict(schema_version=1, action=action, metastore_uri=self.metastore_uri,
                       owner_sha256=self.registry.read()["owner_sha256"], tables=tables,
                       known_tables=sorted(self.registry.read()["tables"]),
                       requested_at=now.isoformat())
        from .real_hive_contract import validate_request
        validate_request(request, now=now)
        body = canonical(request)
        # Contains only registered names, source identity, original deadlines and
        # aggregate hashes/row counts, never aggregate rows or anonymous tokens.
        directory = self.registry.manager.directory / "hive-requests"
        directory.mkdir(exist_ok=True)
        if directory.resolve() != directory.absolute():
            raise ValueError("Hive metadata path traverses a link")
        target = directory / (digest(body) + ".json")
        atomic_write(target, body)
        relative = target.relative_to(self.root).as_posix()
        try:
            result = self.runner(["bash", "tools/spark_hive_catalog.sh", relative, digest(body)], cwd=self.root,
                                 capture_output=True, text=True, timeout=420, check=True)
        finally:
            # Metadata only; deletion is exact and never touches table data.
            target.unlink(missing_ok=True)
        lines = [line.removeprefix("SNOW_HIVE_RESULT=") for line in result.stdout.splitlines() if line.startswith("SNOW_HIVE_RESULT=")]
        if len(lines) != 1:
            raise ValueError("Actual Spark catalog execution did not return one receipt")
        value = json.loads(lines[0])
        if (value.get("request_sha256") != digest(body) or value.get("engine") != "Spark 3.5.7" or
                value.get("master") != "local[1]" or value.get("metastore_uri") != self.metastore_uri or
                not re.fullmatch(r"local-[0-9]+", value.get("application_id", "")) or
                value.get("action") != action or value.get("source") != "real" or
                not now <= timestamp(value["checked_at"]) <= now + timedelta(minutes=10)):
            raise ValueError("Actual catalog engine or request binding differs")
        return value
