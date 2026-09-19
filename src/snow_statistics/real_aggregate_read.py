"""Actual remote cleanup plus local retention before private aggregate reads."""
import os
import socket
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta

from .io import digest
from .landing import checked_receipt, load
from .lifecycle import RealLifecycle, timestamp
from .publication import canonical
from .real_hive import HiveRegistry, HiveRetention, SparkCatalog, plan
from .real_lab import hdfs_context, secret_file, validate_config, writer_registry
from .real_publication import read_real_release
from .real_remote_lifecycle import RealRemoteLifecycle


def read_managed_aggregate(config, run_id, *, root):
    """Never starts services; unavailable resource phases fail closed to the UI."""
    validate_config(config)
    if os.name != "posix" or socket.gethostname() != config["transport_node"] or str(root) != "/home/snow/Snow_Statistics":
        raise ValueError("Private aggregate admission runs on the configured transport VM")
    from .real_transfer import paths
    relative = paths(config["lane"], run_id)
    directory = root / relative["published"] if config["input_origin"] == "real" else root / "runtime/real/publication"
    manager = RealRemoteLifecycle(root / "runtime/real/lifecycle" / config["lane"])
    ods = root / "runtime/real/ods" / config["lane"]
    state = load(ods / "state.json")
    if state is None:
        raise ValueError("A missing durable ODS head cannot authorize existing aggregate reads")
    snapshot = checked_receipt(state)
    with hdfs_context(config) as (sink, hdfs):
        from .real_backend_lifecycle import backend_adapters
        context = (backend_adapters(secret_file(root, config["backend_config_file"]), hdfs)
                   if config["backend_config_file"] else nullcontext({}))
        if config["input_origin"] == "real":
            registry = writer_registry(config, root)
            if timestamp(registry.epoch.read()["expires_at"]) <= datetime.now(UTC):
                from .real_retired import RetiredStorage
                context = nullcontext(RetiredStorage(registry.epoch, snapshot).adapters())
            else:
                from .real_quiescent import StoppedStorage
                context = nullcontext(StoppedStorage(registry, snapshot).adapters())
        with context as checks:
            _, registered = manager._read()
            if registered["backends"]["hive"]["state"] == "initialized" or (manager.directory / "hive-catalog.json").exists():
                registry = HiveRegistry(manager)
                checks["hive"] = HiveRetention(registry, SparkCatalog(root, registry, config["nodes"]["snow-control"]))
            return read_checked(directory, run_id, manager, hdfs, lambda: manager.cleanup(hdfs, ods, sink, backend_checks=checks),
                                input_origin=config["input_origin"])


def read_checked(directory, run_id, manager, hdfs, cleanup, *, input_origin, clock=lambda: datetime.now(UTC)):
    """Trusted in-process adapters only; there is no CLI for a success receipt."""
    result = cleanup()
    current = clock()
    if manager.journal.exists():
        raise ValueError("Failed remote cleanup blocks private aggregate reads")
    if not current - timedelta(minutes=10) <= timestamp(result["checked_at"]) <= current:
        raise ValueError("Remote aggregate read check is stale")
    # Local copy's own registered 90-day cleanup and checksum remain mandatory.
    release = read_real_release(directory, current)
    if release["run_id"] != run_id:
        raise ValueError("Managed aggregate run differs from the explicit read target")
    owner, registered = manager._read()
    if result["registry_sha256"] != digest(canonical(registered)):
        raise ValueError("Remote registered scope changed after cleanup")
    descriptors = plan(release, owner, registered["artifacts"], now=current)
    for item in descriptors.values():
        if not hdfs.exists(item["location"]):
            raise ValueError("The original registered aggregate location is missing")
    local = RealLifecycle(directory / "data")
    local_state = local._read()
    local_gate = load(local.root / "gate.json")
    if local_gate.get("open") is not True:
        raise ValueError("Local cleanup changed during aggregate admission")
    limit = min(current + timedelta(seconds=60), timestamp(release["expires_at"]))
    if result["next_expiry"]:
        limit = min(limit, timestamp(result["next_expiry"]))
    if local_gate.get("next_expiry"):
        limit = min(limit, timestamp(local_gate["next_expiry"]))
    if limit <= clock():
        raise ValueError("Aggregate admission expired during verification")
    receipt = dict(schema_version=1, source="real", input_origin=input_origin, kind="aggregate_read_only",
                   run_id=run_id, checked_at=current.isoformat(), expires_at=limit.isoformat(),
                   aggregate_expires_at=release["expires_at"], cutoff=release["daily"]["manifest"]["cutoff"],
                   collector=release["daily"]["manifest"]["input_snapshot"]["collector"],
                   release_sha256=digest(canonical(release)), remote_receipt_sha256=digest(canonical(result)),
                   local=dict(root=str(local.root), registry_sha256=digest(canonical(local_state)),
                              owner_sha256=digest(local.owner.read_bytes())),
                   remote=dict(directory=str(manager.directory), registry_sha256=result["registry_sha256"],
                               owner_sha256=digest(canonical(owner))),
                   raw_compute_allowed=False, writer_restore_allowed=False)
    return release, receipt


def cached_readable(release, receipt, *, now=None):
    """At most sixty seconds, also bounded by original aggregate/remote expiry."""
    current = now or datetime.now(UTC)
    if (receipt.get("kind") != "aggregate_read_only" or receipt.get("source") != "real" or
            receipt.get("raw_compute_allowed") is not False or receipt.get("writer_restore_allowed") is not False or
            receipt.get("release_sha256") != digest(canonical(release)) or receipt.get("aggregate_expires_at") != release["expires_at"] or
            not timestamp(receipt["checked_at"]) <= current < timestamp(receipt["expires_at"]) <= timestamp(receipt["checked_at"]) + timedelta(seconds=60) or
            timestamp(receipt["expires_at"]) > timestamp(release["expires_at"])):
        raise ValueError("Private aggregate read admission expired or changed")
    # Inspect only this registered publication/lane's small metadata files on
    # each render, never a recursive repository or runtime inventory. A new
    # failed cleanup or changed scope invalidates the in-process lease at once.
    local = RealLifecycle(receipt["local"]["root"])
    state = local._read()
    gate = load(local.root / "gate.json")
    if (gate.get("open") is not True or
            digest(canonical(state)) != receipt["local"]["registry_sha256"] or
            digest(local.owner.read_bytes()) != receipt["local"]["owner_sha256"] or
            gate.get("next_expiry") and timestamp(gate["next_expiry"]) <= current):
        raise ValueError("Local aggregate cleanup gate closed or its registered scope changed")
    manager = RealRemoteLifecycle(receipt["remote"]["directory"])
    owner, registered = manager._read()
    if (manager.journal.exists() or
            digest(canonical(registered)) != receipt["remote"]["registry_sha256"] or
            digest(canonical(owner)) != receipt["remote"]["owner_sha256"]):
        raise ValueError("Remote aggregate cleanup gate closed or its registered scope changed")
    return release
