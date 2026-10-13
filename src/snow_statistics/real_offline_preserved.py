"""Explicit offline compatibility for stopped failed03 beside paused current04.

This is a metadata observation plus actual Docker admission, never a replacement
pause/retirement receipt or read permit. No other failed epoch can be selected.
"""
import argparse
import json
import os
import re
import signal
import socket
import stat
from contextlib import ExitStack, contextmanager
from datetime import UTC, datetime
from pathlib import Path

from .io import digest, write_json
from .landing import collector_identity
from .publication import canonical, publication_lock
from .real_epoch import Epoch, expire_due
from .real_lab import REMOTE_ROOT, ROOT, private_relative, read_json
from .real_quiescent import DockerStorage, WriterRegistry, storage

FAILED, CURRENT = "real-prod-03", "real-prod-04"
DATANODE = "snow-lab-analysis-datanode-1"


def selection(failed, checksum):
    if failed is None and checksum is None:
        return None
    if failed != FAILED or not isinstance(checksum, str) or not re.fullmatch(r"[a-f0-9]{64}", checksum):
        raise ValueError("Select only the observed failed03 scope and its exact metadata SHA256")
    return dict(failed_epoch=failed, state_sha256=checksum)


def existing(path, *, directory=False):
    path = Path(path).absolute()
    info = path.stat()
    if path.resolve() != path or (not stat.S_ISDIR(info.st_mode) if directory else
                                 not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > 4 * 1024**2):
        raise ValueError("Expected a bounded existing unlinked scope")
    return path


@contextmanager
def scope_locks(root):
    # Existing-only paths: observation cannot create a missing registration.
    with ExitStack() as locks:
        for relative in ("epochs/" + FAILED, "epochs/" + FAILED + "/writer-operation",
                         "sync/" + FAILED, "epochs/" + CURRENT + "/writer-operation"):
            folder = existing(Path(root) / "runtime/real" / relative, directory=True)
            existing(folder / "publisher.lock")
            locks.enter_context(publication_lock(folder))
        yield


def failed_local(root, expected_collector):
    folder = existing(Path(root) / "runtime/real/sync" / FAILED, directory=True)
    if {path.name for path in folder.iterdir()} != {"source.json", "target.json", "cursor.json", "rebase.json", "publisher.lock"}:
        raise ValueError("Failed03 has payload, pending input, gap or unknown local state")
    ods = Path(root) / "runtime/real/ods" / FAILED
    if ods.exists() or ods.is_symlink():
        raise ValueError("Failed03 already has an ODS scope")
    values = {name: read_json(existing(folder / name)) for name in ("source.json", "target.json", "cursor.json", "rebase.json")}
    source, target, cursor, rebase = (values[k] for k in values)
    if (collector_identity(source) != expected_collector or rebase.get("source") != source or
            cursor != {"cursor": rebase.get("after")} or type(cursor["cursor"]) is not int or cursor["cursor"] < 0 or
            target.get("source") != "real" or target.get("lane") != FAILED.replace("-", "_") or
            rebase.get("new_lane") != target["lane"] or rebase.get("coverage") != "starts_after_explicit_gap"):
        raise ValueError("Failed03 source/rebase prefix differs from its original registration")
    return {name: digest(existing(folder / name).read_bytes()) for name in values}


def observed_state(config, root, docker, *, now=None):
    """Caller holds scope locks; read actual identities without reading row data."""
    from .real_offline_small import checked_config
    config, root = checked_config(config), Path(root)
    if config["lane"] != CURRENT:
        raise ValueError("This compatibility candidate binds only failed03 and current04")
    current = now or datetime.now(UTC)
    directory = existing(root / "runtime/real/epochs", directory=True)
    folders = sorted(directory.iterdir())
    if len(folders) > 32 or not {FAILED, CURRENT} <= {path.name for path in folders}:
        raise ValueError("Expected bounded original epoch registry")
    result = {}
    for folder in folders:
        existing(folder, directory=True)
        epoch = Epoch(folder, docker)
        manifest = epoch.read()
        if (folder / "retirement.json").exists():
            retired = read_json(existing(folder / "retirement.json"))
            if (folder.name in {FAILED, CURRENT} or retired.get("owner_manifest_sha256") != digest(canonical(manifest)) or
                    set(manifest["containers"].values()) & docker.containers() or
                    set(manifest["volumes"].values()) & docker.volumes()):
                raise ValueError("Retired scope changed or the selected scope is no longer eligible")
            result[folder.name] = dict(manifest_sha256=digest(canonical(manifest)),
                                      retirement_sha256=digest((folder / "retirement.json").read_bytes()))
            continue
        registry = WriterRegistry(epoch)
        identity = collector_identity(read_json(existing(root / "runtime/real/sync" / folder.name / "source.json")))
        registered = registry.ready(identity, now=current)
        history = registry.recovery(now=current)
        if storage(epoch, running=False) != registered["storage"]:
            raise ValueError("Every unretired scope needs its unchanged stopped actual storage")
        if folder.name == FAILED:
            if history:
                raise ValueError("Failed03 is no longer the original abandoned writer without recovery")
            local = failed_local(root, registered["collector"])
        elif not history or history[-1]["action"] != "paused":
            raise ValueError("Every other unretired writer must have completed its actual pause")
        result[folder.name] = dict(generation=registered["epoch_generation"],
            original_min_accepted_at=registered["original_min_accepted_at"], expires_at=registered["expires_at"],
            storage_sha256=digest(canonical(registered["storage"])),
            metadata_sha256={name: digest(existing(folder / name).read_bytes()) for name in
                             ("manifest.json", "writer-registration.json", "writer-job.json")},
            state="abandoned_stopped" if folder.name == FAILED else "paused")
        if folder.name == FAILED:
            result[folder.name]["local_input_metadata_sha256"] = local
    return dict(schema_version=1, source="real", failed_epoch=FAILED, current_epoch=CURRENT,
                config_sha256=digest(canonical(config)), epochs=result,
                failed_local_input_chain_absent=True, kafka_empty_claimed=False,
                read_permission=False, retirement_or_recovery_created=False)


def verify_selection(config, root, chosen, *, docker=None):
    chosen = selection(chosen["failed_epoch"], chosen["state_sha256"])
    with scope_locks(root):
        observed = observed_state(config, root, docker or DockerStorage())
        if digest(canonical(observed)) != chosen["state_sha256"]:
            raise ValueError("Observed original preserved scope changed; do not reuse its digest")
    return dict(preserved_failed_epoch=FAILED, preserved_state_sha256=chosen["state_sha256"],
                retained_state="abandoned_stopped", read_permission=False)


def datanode_identity(root, docker, *, running):
    from .real_offline_small import container_ownership, parent_volume_readback
    value = json.loads(docker.command(["inspect", DATANODE], timeout=10))[0]
    labels, host, state = value["Config"].get("Labels") or {}, value["HostConfig"], value["State"]
    if (not re.fullmatch(r"[a-f0-9]{64}", value["Id"]) or state["Running"] is not running or
            state.get("Paused") or state.get("Restarting") or state.get("Dead") or
            host["RestartPolicy"]["Name"] not in ("", "no") or host.get("Privileged") or
            host["NetworkMode"] != "host" or not 0 < host["Memory"] <= 768 * 1024**2 or
            value["Config"]["Entrypoint"] != ["hdfs", "datanode"] or value["Config"].get("Cmd") not in (None, [])):
        raise ValueError("Existing DataNode has unexpected state or service configuration")
    projected = dict(id=value["Id"], pid=state.get("Pid", 0), name=value["Name"], image=value["Config"]["Image"],
                     image_id=value["Image"], project=labels.get("com.docker.compose.project"),
                     service=labels.get("com.docker.compose.service"),
                     config_files=labels.get("com.docker.compose.project.config_files"),
                     working_dir=labels.get("com.docker.compose.project.working_dir"), mounts=value["Mounts"])
    identity = container_ownership(root, "snow-analysis", DATANODE, projected, parent_probe=parent_volume_readback)
    volumes = {mount["Name"]: docker.inspect_volume(mount["Name"]) for mount in value["Mounts"] if mount["Type"] == "volume"}
    for name, volume in volumes.items():
        if volume["Driver"] != "local" or volume.get("Options") or not volume.get("CreatedAt"):
            raise ValueError("DataNode volume ownership or creation metadata differs")
        consumers = docker.command(["ps", "-a", "--no-trunc", "--filter", "volume=" + name, "--format", "{{.ID}}"], timeout=10)
        if consumers.decode().splitlines() != [value["Id"]]:
            raise ValueError("Another container uses the DataNode storage")
    return dict(container_id=value["Id"], ownership_sha256=identity, created_at=value["Created"],
                service_sha256=digest(canonical(dict(config=value["Config"], host=host))),
                volumes_sha256=digest(canonical(volumes)))


def checked_worker(directory, config, chosen):
    from .real_offline_small import process_identity
    worker = read_json(existing(directory / "worker.json"))
    if (set(worker) != {"config_sha256", "phase", "run_id", "process", "preserved_failed"} or
            worker["config_sha256"] != digest(canonical(config)) or worker["phase"] != "node-start-offline" or
            worker["run_id"] is not None or worker["preserved_failed"] != chosen):
        raise ValueError("Preserved start does not belong to this supervised worker")
    if ((directory / "cancelled.json").exists() or worker["process"].get("pid") != os.getppid() or
            process_identity(os.getppid()) != worker["process"]):
        raise ValueError("Preserved service child has no live unchanged controller parent")
    return worker


def stop_service(root, directory, worker, *, docker=None):
    """Cancellation ignores expiry/admission; only its durable exact daemon scope."""
    path = directory / "preserved-service.json"
    with publication_lock(directory / "preserved-service-cleanup"):
        if not path.exists():
            return dict(status="never_started", data_deleted=False)
        scope = read_json(existing(path))
        if (set(scope) != {"schema_version", "worker_sha256", "identity"} or scope["schema_version"] != 1 or
                scope["worker_sha256"] != digest(canonical(worker))):
            raise ValueError("Daemon cleanup scope belongs to a different worker")
        identity = scope["identity"]
        if (not isinstance(identity, dict) or set(identity) != {"container_id", "ownership_sha256", "created_at", "service_sha256", "volumes_sha256"}
                or any(not isinstance(identity[key], str) or not re.fullmatch(r"[a-f0-9]{64}", identity[key])
                       for key in ("container_id", "ownership_sha256", "service_sha256", "volumes_sha256"))
                or not isinstance(identity["created_at"], str)):
            raise ValueError("Invalid exact daemon cleanup identity")
        done = directory / "preserved-service-stopped.json"
        if done.exists():
            value = read_json(existing(done))
            if value != dict(scope_sha256=digest(canonical(scope)), running=False, data_deleted=False):
                raise ValueError("Daemon stop receipt changed")
            return dict(status="already_stopped_by_this_attempt", data_deleted=False)
        docker = docker or DockerStorage()
        identifier = scope["identity"]["container_id"]
        # Reject absence, replacement and daemon connectivity failures alike;
        # this compatibility path never removes the original DataNode.
        raw = json.loads(docker.command(["inspect", identifier], timeout=10))[0]
        actual = datanode_identity(root, docker, running=raw["State"]["Running"])
        if actual != scope["identity"]:
            raise ValueError("Cannot stop a replaced DataNode")
        if raw["State"]["Running"]:
            docker.command(["stop", "--time", "15", identifier], timeout=25)
        if datanode_identity(root, docker, running=False) != scope["identity"]:
            raise RuntimeError("Exact DataNode did not stop unchanged")
        write_json(done, dict(scope_sha256=digest(canonical(scope)), running=False, data_deleted=False))
        return dict(status="stopped", data_deleted=False)


def start_service(config, root, chosen, attempt, *, docker=None):
    from .real_offline_small import attempt_path, check_guest, guest_sample
    from .real_transfer import cleanup_copies
    docker, root = docker or DockerStorage(), Path(root)
    directory = existing(attempt_path(root, attempt), directory=True)
    worker = checked_worker(directory, config, chosen)
    cleanup_copies(root)
    expire_due(root / "runtime/real/epochs", docker)
    scope_path = directory / "preserved-service.json"
    if scope_path.exists():
        raise ValueError("Use a fresh attempt; service ownership is immutable")
    started = False
    try:
        with scope_locks(root):
            before = observed_state(config, root, docker)
            if digest(canonical(before)) != chosen["state_sha256"]:
                raise ValueError("Preserved original scope changed before service admission")
            sample = guest_sample(root, "snow-analysis", config)
            check_guest(sample, "snow-analysis")
            if sample["containers"]:
                raise ValueError("Preserved service start requires all analysis containers stopped")
            identity = datanode_identity(root, docker, running=False)
            # Durable before the only start mutation; cancellation can always
            # stop this exact ID even if the detached child is killed next.
            write_json(scope_path, dict(schema_version=1, worker_sha256=digest(canonical(worker)), identity=identity))
            started = True
            checked_worker(directory, config, chosen)
            docker.command(["start", identity["container_id"]], timeout=30)
            if datanode_identity(root, docker, running=True) != identity:
                raise RuntimeError("DataNode identity changed during start")
            check_guest(guest_sample(root, "snow-analysis", config), "snow-analysis")
            if observed_state(config, root, docker) != before:
                raise ValueError("Original epoch scope changed during offline service start")
        return dict(node="snow-analysis", offline_services="up", preserved_failed_epoch=FAILED,
                    preserved_state_sha256=chosen["state_sha256"], current_writer_paused=True,
                    data_deleted_by_stop=False, lifecycle_permit=False)
    except BaseException:
        if started:
            stop_service(root, directory, worker, docker=docker)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("phase", choices=("inspect", "start"))
    parser.add_argument("--preserve-stopped-epoch", choices=(FAILED,))
    parser.add_argument("--preserved-state-sha256")
    parser.add_argument("--attempt")
    args = parser.parse_args()
    if os.name != "posix" or socket.gethostname() != "snow-analysis" or ROOT != Path(REMOTE_ROOT):
        raise ValueError("Use only the owned analysis checkout")
    private_relative(args.config, "config", ".json")
    config = read_json(ROOT / args.config)
    if args.phase == "inspect":
        if args.attempt or args.preserve_stopped_epoch or args.preserved_state_sha256:
            parser.error("Inspect observes the fixed pair without accepting a success flag")
        with scope_locks(ROOT):
            state = observed_state(config, ROOT, DockerStorage())
        result = dict(state_sha256=digest(canonical(state)), failed_epoch=FAILED, current_epoch=CURRENT,
                      observed_at=datetime.now(UTC).isoformat(), read_permission=False,
                      kafka_empty_claimed=False, files_written=False)
    else:
        chosen = selection(args.preserve_stopped_epoch, args.preserved_state_sha256)
        if chosen is None:
            parser.error("Explicit preserved scope digest is required")
        def interrupted(*_):
            raise InterruptedError("Preserved offline service worker interrupted")
        for sig in (signal.SIGTERM, signal.SIGHUP):
            signal.signal(sig, interrupted)
        result = start_service(config, ROOT, chosen, args.attempt)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
