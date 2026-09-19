"""Finite Windows orchestration for explicitly selected small offline profiles.

This module does not replace writer admission, lifecycle permits or job validation.
Its Linux entry points are fixed probes and supervised calls to the frozen runner.
"""
import argparse
import errno
import importlib.util
import json
import os
import re
import select
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from .io import digest, write_json
from .publication import canonical, publication_lock
from .real_lab import (
    JOB_PHASES,
    NODES,
    REMOTE_ROOT,
    ROOT,
    SERVICES,
    Runner,
    metadata_paths,
    private_relative,
    read_json,
    route,
    validate_config,
)

PROFILE = "real-small-1920"
PROFILES = {
    PROFILE: {"snow-control": 2048, "snow-compute": 1920, "snow-analysis": 768},
    "real-small-1792": {"snow-control": 2048, "snow-compute": 1792, "snow-analysis": 768},
}
MEMORY = PROFILES[PROFILE]
PHASES = ("status", "start-offline", "stop-offline", "land", "prepare", "cleanup", "permit",
          "stage-compute", "daily", "behavior", "validate", "publish-private", "stage-release")
REMOTE_PHASES = set(PHASES) - {"status", "start-offline", "stop-offline", "stage-compute", "stage-release"}
REMOTE_PHASES |= {"node-start-offline", "node-stop-offline", "metadata-directories", "export-release",
                  "release-directories", "reserve-release", "import-release"}
MIB, GIB = 1024**2, 1024**3
STOP_BYTES = 255 * GIB // 4
LIMIT_BYTES = 64 * GIB
LOG_LIMIT = MIB
COLD_WRITE_BUDGET_MIB = 128


class NodeCommandFailure(RuntimeError):
    def __init__(self, code):
        self.returncode = code
        super().__init__("Owned local/SSH command failed")


def heartbeat_pipe_closed(error):
    """Windows CRT reports EINVAL when flushing an already closed child pipe."""
    return (isinstance(error, BrokenPipeError) or error.errno == errno.EPIPE
            or (os.name == "nt" and error.errno == errno.EINVAL))


def checked_config(value):
    value = validate_config(value)
    if value["input_origin"] != "real" or value["transport_node"] != "snow-analysis":
        raise ValueError("This entry point is only for the registered production real lane")
    return value


def attempt_path(root, attempt):
    if not isinstance(attempt, str) or not re.fullmatch(r"[a-f0-9]{32}", attempt):
        raise ValueError("An exact controller-generated attempt ID is required")
    result = Path(root) / "runtime/real/offline-small" / attempt
    if result.resolve() != result.absolute():
        raise ValueError("Offline operation metadata cannot traverse links")
    return result


def profile_memory(profile):
    if profile not in PROFILES:
        raise ValueError("Select an explicit reviewed small offline profile")
    return dict(PROFILES[profile])


def description(phase, run_id=None, profile=PROFILE):
    if phase not in PHASES:
        raise ValueError("Unknown explicit offline phase")
    memory = profile_memory(profile)
    return dict(phase=phase, run_id=run_id, profile=profile, memory_mib=memory,
                start_reserve_mib=256, project_stop_bytes=STOP_BYTES, project_hard_bytes=LIMIT_BYTES,
                cold_vm_backing_mib=sum(memory.values()), cold_write_budget_mib=COLD_WRITE_BUDGET_MIB,
                host_disk_min_bytes=35 * GIB, host_ram_min_mib=4096,
                guest_ram_min_mib=128, guest_disk_min_mib=384,
                executes=False, new_permit_implied=False, data_deleted_by_stop=False)


def check_host(value, reserve_mib=0):
    if set(value) != {"project_bytes", "host_free_bytes", "host_available_mib"} or any(
            type(v) is not int or v < 0 for v in value.values()):
        raise ValueError("Unexpected actual host resource sample")
    if (value["project_bytes"] >= STOP_BYTES or value["project_bytes"] + reserve_mib * MIB > LIMIT_BYTES
            or value["host_free_bytes"] < 35 * GIB or value["host_available_mib"] < 4096 + reserve_mib):
        raise RuntimeError("Host resource gate failed; retain data and stop owned offline work")
    return value


def check_cold_start(value, profile):
    check_host(value)
    projected = value["project_bytes"] + (sum(profile_memory(profile).values()) + COLD_WRITE_BUDGET_MIB) * MIB
    if projected > STOP_BYTES:
        raise RuntimeError("Cold VM backing plus 128 MiB write budget exceeds the 63.75 GiB stop line")
    return dict(projected_bytes=projected, write_budget_mib=COLD_WRITE_BUDGET_MIB)


def host_sample(root):
    used = 0
    for path in Path(root).rglob("*"):
        try:
            if path.is_file():
                used += path.stat().st_size
        except FileNotFoundError:
            continue
    result = subprocess.run(["powershell", "-NoProfile", "-Command",
                             "(Get-CimInstance Win32_OperatingSystem).FreePhysicalMemory"],
                            capture_output=True, text=True, check=True, timeout=15)
    return dict(project_bytes=used, host_free_bytes=shutil.disk_usage(root).free,
                host_available_mib=int(result.stdout.strip()) // 1024)


def expected_containers(node):
    return {"snow-lab-" + node.removeprefix("snow-") + "-" + service + "-1" for service in SERVICES[node]}


def check_guest(value, node, *, allow_driver=False, enforce_resources=True):
    if (not isinstance(value, dict) or set(value) != {"node", "available_ram_mib", "disk_free_mib", "containers"} or value["node"] != node
            or type(value["available_ram_mib"]) is not int or type(value["disk_free_mib"]) is not int
            or not isinstance(value["containers"], dict)):
        raise ValueError("Unexpected guest resource sample")
    allowed = expected_containers(node) | ({"snow-spark-yarn"} if node == "snow-control" and allow_driver else set())
    if not set(value["containers"]) <= allowed:
        raise ValueError("Unknown running work prevents taking ownership of this VM")
    for name, item in value["containers"].items():
        if isinstance(item, dict) and item.get("ownership_sha256") is None:
            raise ValueError("Exact container ownership was rejected; inspect pinned image and mount metadata")
        if (not isinstance(item, dict) or set(item) != {"id", "ownership_sha256", "memory_current", "oom", "oom_kill"}
                or not isinstance(item["id"], str) or not isinstance(item["ownership_sha256"], str)
                or not re.fullmatch(r"[a-f0-9]{64}", item["id"])
                or not re.fullmatch(r"[a-f0-9]{64}", item["ownership_sha256"])
                or any(type(item[k]) is not int or item[k] < 0 for k in ("memory_current", "oom", "oom_kill"))):
            raise ValueError("Guest cgroup metadata is not exact")
        if enforce_resources and (item["oom"] or item["oom_kill"]):
            raise RuntimeError("An owned offline container has recorded an OOM event")
    if enforce_resources and (value["available_ram_mib"] < 128 or value["disk_free_mib"] < 384):
        raise RuntimeError("Guest resource gate failed")
    return value


def parent_volume_readback(value, mount):
    """Actual image declaration and exact volume metadata; no environment fields."""
    def query(arguments):
        raw = subprocess.check_output(["sudo", "docker", *arguments], timeout=10)
        if len(raw) > 65536:
            raise ValueError("Parent-volume readback exceeds its metadata bound")
        return raw
    image = json.loads(query(["image", "inspect", "--format",
                             '{"image_id":{{json .Id}},"declared_volumes":{{json .Config.Volumes}}}', value["image_id"]]))
    volume = json.loads(query(["volume", "inspect", "--format",
                              '{"name":{{json .Name}},"created_at":{{json .CreatedAt}},"driver":{{json .Driver}},'
                              '"mountpoint":{{json .Mountpoint}},"scope":{{json .Scope}},"options":{{json .Options}}}', mount["Name"]]))
    consumers = query(["ps", "-a", "--no-trunc", "--filter", "volume=" + mount["Name"], "--format", "{{.ID}}"])
    return dict(image=image, volume=volume, consumers=consumers.decode().splitlines())


def checked_parent_volume(value, mount, evidence):
    """One image-declared /data parent, never an arbitrary anonymous volume."""
    volume_name = mount.get("Name")
    if (mount.get("Type") != "volume" or not isinstance(volume_name, str)
            or not re.fullmatch(r"[a-f0-9]{64}", volume_name) or mount.get("Destination") != "/data"
            or mount.get("Driver") != "local" or mount.get("RW") is not True
            or mount.get("Source") != "/var/lib/docker/volumes/" + volume_name + "/_data"
            or mount.get("Propagation") != ""):
        raise ValueError("Only an exact local anonymous /data volume may come from the locked Hadoop image")
    if (not isinstance(evidence, dict) or set(evidence) != {"image", "volume", "consumers"}
            or evidence["image"] != dict(image_id=value["image_id"], declared_volumes={"/data": {}})
            or evidence["consumers"] != [value["id"]]):
        raise ValueError("The locked image declaration or exclusive parent-volume references differ")
    volume = evidence["volume"]
    if (not isinstance(volume, dict) or set(volume) != {"name", "created_at", "driver", "mountpoint", "scope", "options"}
            or volume["name"] != volume_name or volume["mountpoint"] != mount["Source"]
            or volume["driver"] != "local" or volume["scope"] != "local" or volume["options"] not in (None, {})
            or not isinstance(volume["created_at"], str)):
        raise ValueError("The exact anonymous parent-volume identity changed")
    created = datetime.fromisoformat(volume["created_at"].replace("Z", "+00:00"))
    if created.tzinfo is None or created > datetime.now(UTC):
        raise ValueError("Parent-volume creation time must be an actual timezone-aware past instant")
    return evidence


def container_ownership(root, node, name, value, *, driver_id=None, parent_probe=None):
    """Projection of Docker identity: do not fetch credentials or health logs."""
    root = Path(root)
    project = "snow-lab-" + node.removeprefix("snow-")
    image_locks = dict(line.split("=", 1) for line in (root / "lab/locks/images.env").read_text().splitlines() if "=" in line)
    common = ("bind", str(root / "lab/generated/hadoop"), "/etc/hadoop", False)
    if name == "snow-spark-yarn":
        if node != "snow-control" or value["id"] != driver_id:
            raise ValueError("A Spark name alone is not an owned driver identity")
        image = image_locks["SPARK_IMAGE"]
        mounts = {common, ("bind", str(root), "/opt/snow", True)}
    else:
        service = next((v for v in SERVICES[node] if name == project + "-" + v + "-1"), None)
        files = [str(root / "lab" / ("compose." + node.removeprefix("snow-") + ".yaml"))]
        if node != "snow-analysis":
            files.append(str(root / "lab" / ("compose." + node.removeprefix("snow-") + "-scale.yaml")))
        if (service is None or value["project"] != project or value["service"] != service
                or value["config_files"].split(",") != files or value["working_dir"] != str(root / "lab")):
            raise ValueError("Same-name container is not owned by the exact offline Compose deployment")
        image = "snow-yarn-spark:0.1.0" if service == "nodemanager" else image_locks["HADOOP_IMAGE"]
        mounts = {common}
        if service == "namenode":
            mounts |= {("volume", project + "_namenode", "/data/name", True),
                       ("bind", str(root / "lab/namenode-entrypoint.sh"), "/snow/namenode-entrypoint.sh", False)}
        if service == "datanode":
            mounts.add(("volume", project + "_datanode", "/data/dn", True))
        if service == "nodemanager":
            mounts |= {("volume", project + "_yarn", "/data/yarn", True),
                       ("bind", str(root / "lab/locks/spark-jars.sha256"), "/snow/spark-jars.sha256", False),
                       ("bind", str(root / "lab/nodemanager-entrypoint.sh"), "/snow/nodemanager-entrypoint.sh", False)}
    if (value["name"] != "/" + name or value["image"] != image
            or not re.fullmatch(r"sha256:[a-f0-9]{64}", value["image_id"])):
        raise ValueError("Owned container image or name identity changed")
    parent = [v for v in value["mounts"] if v["Destination"] == "/data"]
    parent_identity = None
    if parent:
        if len(parent) != 1 or name == "snow-spark-yarn" or parent_probe is None:
            raise ValueError("An undeclared parent mount cannot be ignored")
        mount = parent[0]
        # Validate the exact name before it can become a Docker argument.
        if (mount.get("Type") != "volume" or not isinstance(mount.get("Name"), str)
                or not re.fullmatch(r"[a-f0-9]{64}", mount["Name"])):
            raise ValueError("Parent mount is not a bounded anonymous volume")
        parent_identity = checked_parent_volume(value, mount, parent_probe(value, mount))
        mounts.add(("volume", mount["Name"], "/data", True))
    actual = {(v["Type"], v["Name"] if v["Type"] == "volume" else v["Source"], v["Destination"], v["RW"]) for v in value["mounts"]}
    if actual != mounts or len(actual) != len(value["mounts"]):
        raise ValueError("Owned container mount identity changed")
    identity = {k: v for k, v in value.items() if k != "pid"}
    if parent_identity:
        identity["image_parent_volume"] = parent_identity
    return digest(canonical(identity))


def owned_driver(root, config, attempt):
    if attempt is None:
        return None
    record = read_json(attempt_path(root, attempt) / "worker.json")
    if (record.get("config_sha256") != digest(canonical(config)) or record.get("phase") not in {"daily", "behavior"}
            or process_identity(record["process"]["pid"]) != record["process"]):
        raise ValueError("No live exact offline worker owns this Spark driver")
    metadata_paths(config, record["run_id"])
    cid = Path(root) / "runtime/real/runs" / record["run_id"] / "data" / (record["phase"] + ".cid")
    if cid.resolve() != cid.absolute() or cid.stat().st_size > 128:
        raise ValueError("Unregistered Spark driver CID")
    value = cid.read_text().strip()
    if not re.fullmatch(r"[a-f0-9]{64}", value):
        raise ValueError("Invalid exact Spark driver CID")
    return value


def guest_sample(root, node, config, attempt=None):
    """No inspect Env/log payload, shell interpolation, or arbitrary cgroup path."""
    names = subprocess.check_output(["sudo", "docker", "ps", "--format", "{{.Names}}"], timeout=10).decode().splitlines()
    if len(names) > 32:
        raise ValueError("Guest inventory exceeds the bounded offline scope")
    containers = {}
    for name in names:
        if not re.fullmatch(r"[A-Za-z0-9_.-]{1,160}", name):
            raise ValueError("Unexpected Docker name")
        template = ('{"id":{{json .Id}},"pid":{{.State.Pid}},"name":{{json .Name}},'
                    '"image":{{json .Config.Image}},"image_id":{{json .Image}},'
                    '"project":{{json (index .Config.Labels "com.docker.compose.project")}},'
                    '"service":{{json (index .Config.Labels "com.docker.compose.service")}},'
                    '"config_files":{{json (index .Config.Labels "com.docker.compose.project.config_files")}},'
                    '"working_dir":{{json (index .Config.Labels "com.docker.compose.project.working_dir")}},'
                    '"mounts":{{json .Mounts}}}')
        raw = subprocess.check_output(["sudo", "docker", "inspect", "--format", template, name], timeout=10)
        value = json.loads(raw)
        try:
            ownership = container_ownership(root, node, name, value,
                                            driver_id=owned_driver(root, config, attempt) if name == "snow-spark-yarn" else None,
                                            parent_probe=parent_volume_readback)
        except (ValueError, KeyError, TypeError, AttributeError):
            # A readable inventory with wrong ownership is different from an
            # SSH connection failure, and must never authorize fallback stop.
            ownership = None
        if not re.fullmatch(r"[a-f0-9]{64}", value["id"]) or type(value["pid"]) is not int or value["pid"] < 1:
            raise ValueError("Docker cgroup identity is missing")
        relation = Path(f"/proc/{value['pid']}/cgroup").read_text().strip()
        if not relation.startswith("0::/") or "\n" in relation or value["id"] not in relation:
            raise ValueError("Docker PID is not bound to its own cgroup v2 hierarchy")
        group = Path("/sys/fs/cgroup") / relation[4:]
        if group.resolve() != group or not group.is_relative_to("/sys/fs/cgroup"):
            raise ValueError("Unsafe cgroup path")
        events = dict(line.split() for line in (group / "memory.events").read_text().splitlines())
        containers[name] = dict(id=value["id"], ownership_sha256=ownership, memory_current=int((group / "memory.current").read_text()),
                                oom=int(events["oom"]), oom_kill=int(events["oom_kill"]))
    memory = dict(line.split(":", 1) for line in Path("/proc/meminfo").read_text().splitlines())
    return dict(node=node, available_ram_mib=int(memory["MemAvailable"].split()[0]) // 1024,
                disk_free_mib=shutil.disk_usage(root).free // MIB, containers=containers)


def paused_guard(config, root):
    from .landing import collector_identity
    from .real_epoch import Epoch
    from .real_quiescent import DockerStorage, WriterRegistry, storage
    cleanup_local_copies(root)
    registry = WriterRegistry(Epoch(Path(root) / "runtime/real/epochs" / config["lane"], DockerStorage()))
    with registry.operation_lock():
        identity = collector_identity(read_json(Path(root) / "runtime/real/sync" / config["lane"] / "source.json"))
        registration = registry.ready(identity)
        history = registry.complete_recovery()
        if not history or history[-1]["action"] != "paused":
            raise ValueError("Finish a registered writer pause before entering the offline stage")
        if storage(registry.epoch, running=False) != registration["storage"]:
            raise ValueError("Stopped real epoch storage identity changed")
        return dict(writer_paused=True, actual_engine_objects_stopped=True,
                    checkpoint_metadata_validated=True, checkpoint_bytes_rechecked=False,
                    lifecycle_permit=False, **input_state(config, root))


def input_state(config, root):
    from .landing import checked_receipt
    directory = Path(root) / "runtime/real/ods" / config["lane"]
    has_pending = (directory / "pending.json").is_file()
    has_landed = False
    if (directory / "state.json").exists():
        has_landed = bool(checked_receipt(read_json(directory / "state.json", 4 * MIB))["batches"])
    return dict(has_pending=has_pending, has_landed=has_landed, has_input=has_pending or has_landed)


def process_identity(pid):
    if type(pid) is not int or pid < 1:
        raise ValueError("Expected an exact process PID")
    try:
        base = Path("/proc") / str(pid)
        fields = (base / "stat").read_text().rsplit(")", 1)[1].split()
        if fields[0] == "Z":
            return None
        return dict(pid=pid, start_ticks=fields[19], argv_sha256=digest((base / "cmdline").read_bytes()))
    except FileNotFoundError:
        return None


def stop_tree(process):
    """Windows wrapper descendants or a Linux session, never process-name killing."""
    if process.poll() is not None:
        return
    if os.name == "nt":
        subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False, timeout=20)
    else:
        os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=15)
    except subprocess.TimeoutExpired:
        if os.name == "nt":
            raise RuntimeError("Owned Windows process tree did not terminate") from None
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=10)


def session_stat(pid):
    """Stable kernel identity, including a zombie leader before it is reaped."""
    if type(pid) is not int or pid < 1:
        raise ValueError("Expected a positive session PID")
    try:
        fields = (Path("/proc") / str(pid) / "stat").read_text().rsplit(")", 1)[1].split()
    except FileNotFoundError:
        return None
    if len(fields) < 20 or not fields[19].isdigit():
        raise ValueError("Invalid POSIX process metadata")
    return dict(pid=pid, state=fields[0], pgid=int(fields[2]), sid=int(fields[3]), start_ticks=fields[19])


def checked_session(identity):
    if (not isinstance(identity, dict) or set(identity) != {"pid", "start_ticks", "pgid", "sid"}
            or any(type(identity[key]) is not int or identity[key] < 1 for key in ("pid", "pgid", "sid"))
            or identity["pgid"] != identity["pid"] or identity["sid"] != identity["pid"]
            or not isinstance(identity["start_ticks"], str) or not re.fullmatch(r"[0-9]+", identity["start_ticks"])):
        raise ValueError("Invalid registered POSIX session")
    return identity


def child_session(pid):
    value = session_stat(pid)
    if value is None:
        raise RuntimeError("New child session identity is unavailable")
    return checked_session({key: value[key] for key in ("pid", "start_ticks", "pgid", "sid")})


def session_members(identity):
    """Never infer absence from the leader's exit, or signal a reused PID."""
    identity = checked_session(identity)
    leader = session_stat(identity["pid"])
    if leader is not None and any(leader[key] != identity[key] for key in identity):
        raise ValueError("Registered session leader identity changed")
    active = []
    for path in Path("/proc").iterdir():
        if not path.name.isascii() or not path.name.isdigit():
            continue
        value = session_stat(int(path.name))
        if value is None or value["pgid"] != identity["pgid"]:
            continue
        if value["sid"] != identity["sid"] or int(value["start_ticks"]) < int(identity["start_ticks"]):
            raise ValueError("Unexpected member in registered child session")
        if value["state"] != "Z":
            active.append(value["pid"])
    return sorted(active)


def stop_session(identity, process=None):
    """Bounded TERM/KILL and actual absence for one registered Linux session."""
    identity = checked_session(identity)
    escalated = False
    for sig, grace in ((signal.SIGTERM, 15), (signal.SIGKILL, 5)):
        if not session_members(identity):
            break
        try:
            os.killpg(identity["pgid"], sig)
        except ProcessLookupError:
            pass
        escalated = escalated or sig == signal.SIGKILL
        until = time.monotonic() + grace
        while session_members(identity) and time.monotonic() < until:
            time.sleep(0.1)
    if session_members(identity):
        raise RuntimeError("Registered child session still has active members")
    if process is not None:
        process.wait(timeout=5)
    return dict(active_members_after_cleanup=0, kill_escalated=escalated)


def checked_child(directory, worker, attempt):
    path = directory / "child.json"
    if path.resolve() != path.absolute():
        raise ValueError("Child session metadata cannot traverse links")
    if not path.exists():
        return None
    child = read_json(path)
    if (set(child) != {"schema_version", "attempt", "worker_sha256", "command_sha256", "session"}
            or type(child["schema_version"]) is not int or child["schema_version"] != 1 or child["attempt"] != attempt
            or child["worker_sha256"] != digest(canonical(worker))
            or not isinstance(child["command_sha256"], str)
            or not re.fullmatch(r"[a-f0-9]{64}", child["command_sha256"])):
        raise ValueError("Child session belongs to another worker attempt")
    checked_session(child["session"])
    return child


def checked_done(directory, worker, child, attempt):
    path = directory / "done.json"
    if path.resolve() != path.absolute():
        raise ValueError("Completion metadata cannot traverse links")
    if not path.exists():
        return None
    done = read_json(path)
    if (set(done) != {"schema_version", "attempt", "worker_sha256", "child_sha256", "launch_attempted",
                     "phase_complete", "child_group_stopped", "driver_stopped", "session_cleanup"}
            or type(done["schema_version"]) is not int or done["schema_version"] != 1 or done["attempt"] != attempt
            or done["worker_sha256"] != digest(canonical(worker))
            or done["child_sha256"] != (digest(canonical(child)) if child else None)
            or any(type(done[key]) is not bool for key in
                   ("launch_attempted", "phase_complete", "child_group_stopped", "driver_stopped"))
            or child is not None and not done["launch_attempted"]
            or done["phase_complete"] and child is None):
        raise ValueError("Completion receipt belongs to another worker attempt")
    if done["child_group_stopped"]:
        if child is not None:
            cleanup = done["session_cleanup"]
            if (not isinstance(cleanup, dict) or set(cleanup) != {"active_members_after_cleanup", "kill_escalated"}
                    or type(cleanup["active_members_after_cleanup"]) is not int
                    or cleanup["active_members_after_cleanup"] != 0 or type(cleanup["kill_escalated"]) is not bool):
                raise ValueError("Invalid child session absence receipt")
        elif done["launch_attempted"] or done["session_cleanup"] is not None:
            raise ValueError("Unregistered child launch cannot certify absence")
    return done


def driver_cleanup(root, phase, run_id):
    if phase not in {"daily", "behavior"}:
        return
    cid = Path(root) / "runtime/real/runs" / run_id / "data" / (phase + ".cid")
    if not cid.exists():
        return
    identifier = cid.read_text().strip()
    if cid.resolve() != cid.absolute() or not re.fullmatch(r"[a-f0-9]{64}", identifier):
        raise ValueError("Cannot cancel an unidentified driver")
    check = subprocess.run(["sudo", "docker", "inspect", "--format", "{{.Id}} {{.Name}}", identifier],
                           capture_output=True, timeout=10)
    if check.returncode:
        if b"No such" not in check.stderr:
            raise RuntimeError("Driver absence cannot be confirmed")
        return
    if check.stdout.decode().strip() != identifier + " /snow-spark-yarn":
        raise ValueError("Driver CID no longer identifies this exact offline driver")
    subprocess.run(["sudo", "docker", "stop", "-t", "10", identifier], check=True,
                   stdout=subprocess.DEVNULL, timeout=30)
    check = subprocess.run(["sudo", "docker", "ps", "--no-trunc", "--filter", "id=" + identifier,
                            "--format", "{{.ID}}"], capture_output=True, check=True, timeout=10)
    if check.stdout.strip():
        raise RuntimeError("Exact offline driver is still running")


def node_cancel(root, config, attempt):
    directory = attempt_path(root, attempt)
    with publication_lock(directory / "admission"):
        path = directory / "worker.json"
        write_json(directory / "cancelled.json", dict(cancelled_at=datetime.now(UTC).isoformat()))
        if not path.exists():
            return dict(status="cancelled_before_worker", data_deleted=False)
        worker = read_json(path)
        if (set(worker) != {"config_sha256", "phase", "run_id", "process"}
                or worker["config_sha256"] != digest(canonical(config)) or worker["phase"] not in REMOTE_PHASES):
            raise ValueError("Cancellation ownership differs from this operation")
        identity = worker["process"]
        if (not isinstance(identity, dict) or set(identity) != {"pid", "start_ticks", "argv_sha256"}
                or type(identity["pid"]) is not int or identity["pid"] <= 0
                or not re.fullmatch(r"[0-9]+", str(identity["start_ticks"]))
                or not re.fullmatch(r"[a-f0-9]{64}", str(identity["argv_sha256"]))):
            raise ValueError("Invalid cancellation process identity")
        if worker["run_id"] is not None:
            metadata_paths(config, worker["run_id"])
        child = checked_child(directory, worker, attempt)
        done = checked_done(directory, worker, child, attempt)
        if done and done["child_group_stopped"] and done["driver_stopped"]:
            # A later attempt may now own a different driver. Never re-stop it
            # merely because an already finished cancellation was retried.
            return dict(status="cancelled", already_cleaned=True, data_deleted=False)
    group_stopped, driver_stopped, cleanup = False, False, None
    try:
        actual = process_identity(identity["pid"])
        if actual is not None and actual != identity:
            raise ValueError("Refuse a reused offline worker PID")
        if actual == identity:
            try:
                os.kill(identity["pid"], signal.SIGTERM)
            except ProcessLookupError:
                # Natural exit after the identity read does not imply that
                # its separately registered child session has also stopped.
                pass
            until = time.monotonic() + 55
            while process_identity(identity["pid"]) == identity and time.monotonic() < until:
                time.sleep(0.25)
            if process_identity(identity["pid"]) == identity:
                raise RuntimeError("Remote worker cancellation has not completed")
        # Read after the worker's finally: it may have registered the child
        # while the first cancellation snapshot was being taken.
        child = checked_child(directory, worker, attempt)
        done = checked_done(directory, worker, child, attempt)
        if done and done["child_group_stopped"] and done["driver_stopped"]:
            return dict(status="cancelled", already_cleaned=True, data_deleted=False)
        if child is None:
            if not done or done["launch_attempted"] or not done["child_group_stopped"]:
                raise RuntimeError("Legacy or interrupted worker has no proven child-session scope")
            group_stopped = True
        else:
            cleanup = stop_session(child["session"])
            group_stopped = True
    finally:
        if not (done and done["child_group_stopped"] and done["driver_stopped"]):
            driver_cleanup(root, worker["phase"], worker["run_id"])
            driver_stopped = True
        else:
            driver_stopped = True
    if group_stopped and driver_stopped:
        write_json(directory / "done.json", dict(schema_version=1, attempt=attempt, worker_sha256=digest(canonical(worker)),
                   child_sha256=digest(canonical(child)) if child else None, launch_attempted=child is not None,
                   phase_complete=bool(done and done["phase_complete"]), child_group_stopped=True,
                   driver_stopped=True, session_cleanup=cleanup))
    return dict(status="cancelled", data_deleted=False)


def phase_result(log, phase):
    """Only propagate the fixed no-input state, never expose private raw receipts."""
    if log.stat().st_size > LOG_LIMIT:
        raise ValueError("Frozen phase result exceeds its private log bound")
    lines = log.read_text(encoding="utf-8").splitlines()
    value = json.loads(next(line for line in reversed(lines) if line.strip()))
    if not isinstance(value, dict):
        raise ValueError("Frozen phase did not return a structured completion result")
    if value.get("status") == "no_computable_input":
        return dict(phase=phase, status="no_computable_input", metrics_fabricated=False)
    return dict(phase=phase, status="completed", frozen_process_exit_code=0,
                backend_receipts="retained in original managed locations")


def cleanup_local_copies(root):
    """New lake copies retain their original 90-day clock on every node."""
    from .real_lake_authority import cleanup_copies
    return cleanup_copies(root)


class ControllerHeartbeat:
    """Poll the owned POSIX pipe without acquiring a buffered stdin lock."""
    def __init__(self, stream):
        self.fd = stream.fileno()
        if type(self.fd) is not int or self.fd < 0:
            raise ValueError("Controller heartbeat requires an exact pipe descriptor")
        self.pending = b""
        self.last_ping = time.monotonic()

    def check(self):
        # The Linux node has one stdin reader. A readiness check followed by a
        # bounded raw read never leaves a daemon thread holding BufferedReader
        # during interpreter shutdown while the SSH controller keeps stdin open.
        if time.monotonic() - self.last_ping > 20:
            raise RuntimeError("Controller heartbeat was lost")
        if select.select([self.fd], [], [], 0)[0]:
            chunk = os.read(self.fd, 64)
            if not chunk:
                raise RuntimeError("Controller heartbeat was lost")
            self.pending += chunk
            while b"\n" in self.pending:
                line, self.pending = self.pending.split(b"\n", 1)
                if line != b"ping":
                    raise RuntimeError("Controller heartbeat was lost")
                self.last_ping = time.monotonic()
            if not b"ping\n".startswith(self.pending):
                raise RuntimeError("Controller heartbeat was lost")
        if time.monotonic() - self.last_ping > 20:
            raise RuntimeError("Controller heartbeat was lost")


def node_execute(config, config_file, root, phase, run_id, attempt, stdin=None):
    """Pipe loss or heartbeat loss stops the whole owned Linux process group."""
    if phase not in REMOTE_PHASES:
        raise ValueError("Unsupported remote phase")
    if phase in JOB_PHASES or phase == "metadata-directories":
        metadata_paths(config, run_id)
    if phase != "node-stop-offline":
        cleanup_local_copies(root)
    directory = attempt_path(root, attempt)
    directory.mkdir(parents=True, exist_ok=True)
    if phase in {"daily", "behavior"} and (Path(root) / "runtime/real/runs" / run_id / "data" / (phase + ".cid")).exists():
        raise ValueError("An old driver CID requires recovery; use a fresh immutable run")
    with publication_lock(directory / "admission"):
        if (directory / "worker.json").exists() or (directory / "cancelled.json").exists():
            raise ValueError("This exact node attempt cannot be reused")
        worker = dict(config_sha256=digest(canonical(config)), phase=phase,
                      run_id=run_id, process=process_identity(os.getpid()))
        write_json(directory / "worker.json", worker)
    cancelled = threading.Event()
    def interrupted(*_):
        # Defer until the new detached child has a durable identity.
        cancelled.set()
    def check_cancelled():
        if cancelled.is_set() or (directory / "cancelled.json").exists():
            raise InterruptedError("Offline worker was cancelled")
    previous = {sig: signal.signal(sig, interrupted) for sig in
                ([signal.SIGTERM, signal.SIGHUP] if hasattr(signal, "SIGHUP") else [signal.SIGTERM])}
    process, session, child, stream = None, None, None, None
    launch_attempted, complete, cleanup = False, False, None
    log = directory / "private.log"
    drain_thread, log_failed = None, threading.Event()
    try:
        heartbeat = ControllerHeartbeat(stdin if stdin is not None else sys.stdin.buffer)
        command = [sys.executable, "tools/real_lab.py", "--config", config_file, phase]
        if run_id:
            command += ["--run-id", run_id]
        stream = log.open("xb")
        with publication_lock(directory / "admission"):
            check_cancelled()
            launch_attempted = True
            process = subprocess.Popen(command, cwd=root, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT, start_new_session=True)
            session = child_session(process.pid)
            child = dict(schema_version=1, attempt=attempt, worker_sha256=digest(canonical(worker)),
                         command_sha256=digest(canonical(command)), session=session)
            write_json(directory / "child.json", child)
        check_cancelled()
        def drain():
            remaining = LOG_LIMIT
            try:
                while chunk := process.stdout.read(16384):
                    stream.write(chunk[:remaining])
                    if len(chunk) > remaining:
                        log_failed.set()
                    remaining = max(0, remaining - len(chunk))
            except Exception:
                log_failed.set()
        drain_thread = threading.Thread(target=drain, daemon=True)
        drain_thread.start()
        deadline = time.monotonic() + 1200
        while process.poll() is None:
            check_cancelled()
            heartbeat.check()
            if time.monotonic() >= deadline or log_failed.is_set():
                raise RuntimeError("Offline phase exceeded its time or retained log bound")
            time.sleep(0.25)
        check_cancelled()
        if process.returncode:
            raise RuntimeError("Frozen offline phase failed; inspect its private bounded log")
        # The leader can exit while a descendant still holds its stdout pipe.
        cleanup = stop_session(session, process)
        drain_thread.join(timeout=5)
        if drain_thread.is_alive() or log_failed.is_set():
            raise RuntimeError("Frozen offline output exceeded its bounded metadata log")
        stream.flush()
        result = phase_result(log, phase)
        complete = True
        return result
    finally:
        group_stopped, driver_stopped = False, False
        try:
            if process is not None:
                if session is None:
                    # No poll/wait has reaped this child yet. Never substitute
                    # its worker parent's PID for this exact session scope.
                    session = child_session(process.pid)
                final = stop_session(session, process)
                cleanup = dict(active_members_after_cleanup=0,
                               kill_escalated=final["kill_escalated"] or bool(cleanup and cleanup["kill_escalated"]))
                group_stopped = child is not None
            elif not launch_attempted:
                group_stopped = True
        finally:
            try:
                driver_cleanup(root, phase, run_id)
                driver_stopped = True
            finally:
                if drain_thread:
                    drain_thread.join(timeout=10)
                drain_stopped = drain_thread is None or not drain_thread.is_alive()
                # Closing a BufferedReader still owned by a blocked drain
                # thread can itself wait without a bound. Retain the failure.
                if drain_stopped:
                    if stream:
                        stream.close()
                    if process is not None and process.stdout is not None:
                        process.stdout.close()
                for sig, handler in previous.items():
                    signal.signal(sig, handler)
                write_json(directory / "done.json", dict(schema_version=1, attempt=attempt,
                           worker_sha256=digest(canonical(worker)), child_sha256=digest(canonical(child)) if child else None,
                           launch_attempted=launch_attempted, phase_complete=complete,
                           child_group_stopped=group_stopped, driver_stopped=driver_stopped, session_cleanup=cleanup))
                if not drain_stopped:
                    raise RuntimeError("Owned frozen output reader did not stop")


class Monitor:
    def __init__(self, host, guests, *, interval=5):
        self.host, self.guests, self.interval = host, guests, interval
        self.stop = threading.Event()
        self.error = None
        self.samples = 0
        self.last = None
        self.thread = None

    def sample(self):
        sample = check_host(self.host())
        for node, value in self.guests():
            check_guest(value, node, allow_driver=True)
        self.last, self.samples = sample, self.samples + 1

    def start(self):
        self.sample()
        def work():
            while not self.stop.wait(self.interval):
                try:
                    self.sample()
                except BaseException as error:
                    self.error = error
                    self.stop.set()
        self.thread = threading.Thread(target=work, name="snow-offline-resource-monitor")
        self.thread.start()

    def check(self):
        if self.error is not None:
            raise RuntimeError("Continuous offline resource monitor failed") from self.error

    def close(self):
        self.stop.set()
        if self.thread is not None:
            self.thread.join(timeout=70)
            if self.thread.is_alive():
                raise RuntimeError("Offline resource monitor did not stop")


class SmallRunner(Runner):
    def __init__(self, config, config_file, root=ROOT, *, profile=PROFILE):
        super().__init__(checked_config(config), config_file, root)
        self.profile, self.memory = profile, profile_memory(profile)
        self.monitor = None
        self.ready_nodes = set()
        self.owned_nodes = set()
        self.inflight = set()
        self.started_nodes = set()
        self.foreign_nodes = set()
        self.identities = {}
        self.attempt = uuid4().hex
        spec = importlib.util.spec_from_file_location("offline_vmware", self.root / "tools/vmware_lab.py")
        self.vm = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.vm)

    def vm_state(self):
        raw = self.vm.run(self.vm.VMWARE / "vmrun.exe", "-T", "ws", "list").splitlines()
        running = {str(Path(v).absolute()).lower() for v in raw[1:]}
        return {node: str(self.vm.RUNTIME / node / (node + ".vmx")).lower() in running for node in NODES}

    def host(self):
        return host_sample(self.root)

    def ssh(self, node, operation, *, phase=None, run_id=None, attempt=None):
        if node not in NODES or operation not in {"probe", "guard", "input", "execute", "cancel"}:
            raise ValueError("Only fixed owned node operations are allowed")
        options = ["-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "StrictHostKeyChecking=yes",
                   "-o", "HostKeyAlias=" + node, "-o", "UserKnownHostsFile=" + str(self.vm.RUNTIME / "known_hosts"),
                   "-i", str(self.vm.RUNTIME / "id_ed25519")]
        args = [".venv/bin/python", "tools/real_offline_small.py", "--config", self.config_file,
                "--config-sha256", digest(canonical(self.config)), "--profile", self.profile,
                "--node-operation", operation, "status"]
        if phase:
            args += ["--node-phase", phase]
        if operation == "probe" and node == "snow-control" and attempt is None:
            attempts = [v for n, v in tuple(self.inflight) if n == node]
            if len(attempts) > 1:
                raise ValueError("Only one owned control worker may run at a time")
            attempt = attempts[0] if attempts else None
        if run_id:
            args += ["--run-id", run_id]
        if attempt:
            args += ["--attempt", attempt]
        return ["ssh", *options, "snow@" + self.config["nodes"][node],
                "cd " + REMOTE_ROOT + " && exec " + shlex.join(args)]

    def run(self, command, *, body=None, output=None, timeout=1200, quiet=False, env=None,
            capture=False, heartbeat=False, monitored=True):
        command = self.direct_transfer(command)
        process = subprocess.Popen(command, cwd=self.root, stdin=subprocess.PIPE if body is not None or heartbeat else subprocess.DEVNULL,
                                   stdout=subprocess.PIPE if capture else output if output else subprocess.DEVNULL,
                                   stderr=subprocess.PIPE if capture else output if output else subprocess.DEVNULL,
                                   env=env, start_new_session=os.name != "nt")
        finished, result = threading.Event(), []
        def collect():
            try:
                result.append(process.communicate(input=body, timeout=timeout) if not heartbeat else process.communicate(timeout=timeout))
            except BaseException as error:
                result.append(error)
            finally:
                finished.set()
        # communicate closes stdin, so heartbeat operations use a separate wait
        # thread and small fixed metadata stdout instead.
        if heartbeat:
            def collect():
                try:
                    process.wait(timeout=timeout)
                    result.append((process.stdout.read() if capture else b"", process.stderr.read() if capture else b""))
                except BaseException as error:
                    result.append(error)
                finally:
                    finished.set()
        worker = threading.Thread(target=collect)
        worker.start()
        heartbeat_open = heartbeat
        try:
            while not finished.wait(0.5):
                if monitored and self.monitor:
                    self.monitor.check()
                if heartbeat_open:
                    try:
                        process.stdin.write(b"ping\n")
                        process.stdin.flush()
                    except OSError as error:
                        if not heartbeat_pipe_closed(error):
                            raise
                        # A node may close stdin just before its actual exit is
                        # collected. Continue monitoring and await that exit;
                        # a closed pipe is never a successful phase receipt.
                        heartbeat_open = False
            if not result or isinstance(result[0], BaseException):
                raise RuntimeError("Owned local/SSH process timed out or failed") from (result[0] if result else None)
            if process.returncode:
                raise NodeCommandFailure(process.returncode)
            if capture:
                if len(result[0][0]) > 65536:
                    raise ValueError("Node metadata response exceeded its bound")
                return result[0][0]
        finally:
            try:
                stop_tree(process)
            finally:
                if heartbeat and process.stdin:
                    try:
                        process.stdin.close()
                    except OSError as error:
                        if not heartbeat_pipe_closed(error):
                            raise
                worker.join(timeout=20)
                if worker.is_alive():
                    raise RuntimeError("Owned subprocess reader did not stop")

    def direct_transfer(self, command):
        """Inherited metadata/pair staging gets direct SCP, not a hidden SSH child."""
        if len(command) < 2 or command[1] != "tools/lab_remote.py":
            return command
        if len(command) != 8 or command[2] != "--node" or command[3] not in NODES or command[6] != "--remote":
            raise ValueError("Only fixed inherited metadata/aggregate transfers are supported")
        direction, local, remote = command[4:6] + command[7:8]
        if direction not in {"--upload", "--download"} or not remote.startswith(REMOTE_ROOT + "/runtime/real/"):
            raise ValueError("SCP may transfer only the existing registered real metadata/aggregate scope")
        local = Path(local).absolute()
        if local.resolve() != local or not local.is_relative_to(self.root / "runtime/real"):
            raise ValueError("SCP local path escaped ignored real storage")
        if not re.fullmatch(re.escape(REMOTE_ROOT) + r"/runtime/real/[A-Za-z0-9_./-]+", remote) or ".." in Path(remote).parts:
            raise ValueError("Invalid bounded SCP path")
        ssh = self.ssh(command[3], "probe")
        target = ssh[-2] + ":" + remote
        return ["scp", *ssh[1:-2], *([str(local), target] if direction == "--upload" else [target, str(local)])]

    def probe(self, node, operation="probe", *, monitored=False):
        try:
            return json.loads(self.run(self.ssh(node, operation), capture=True, timeout=20, monitored=monitored))
        except NodeCommandFailure as error:
            if error.returncode != 255:
                raise ValueError("Remote authority/probe rejected the operation; it is not a cold-boot SSH retry") from error
            raise

    def guests(self):
        for node in sorted(self.ready_nodes):
            if self.monitor and self.monitor.stop.is_set():
                return
            value = self.probe(node)
            self.remember_objects(node, value)
            yield node, value

    def remember_objects(self, node, value):
        current = {name: (item["id"], item["ownership_sha256"]) for name, item in value["containers"].items()
                   if name != "snow-spark-yarn"}
        before = self.identities.setdefault(node, {})
        if any(name in before and before[name] != identity for name, identity in current.items()):
            self.foreign_nodes.add(node)
            raise ValueError("An observed offline container was replaced; do not stop its VM")
        before.update(current)

    def remote(self, node, phase, run_id=None):
        if phase not in REMOTE_PHASES:
            raise ValueError("This phase is outside the fixed offline scope")
        attempt = uuid4().hex
        self.inflight.add((node, attempt))
        try:
            return json.loads(self.run(self.ssh(node, "execute", phase=phase, run_id=run_id, attempt=attempt),
                                       capture=True, heartbeat=True, timeout=1230,
                                       monitored=phase != "node-stop-offline"))
        finally:
            self.run(self.ssh(node, "cancel", attempt=attempt), timeout=80, monitored=False)
            self.inflight.discard((node, attempt))

    def vm_command(self, action, node):
        command = [sys.executable, "tools/vmware_lab.py", action, "--node", node]
        if action == "configure":
            command += ["--profile", self.profile]
        if action == "start":
            check_host(self.host(), self.memory[node] + 256)
            command += ["--reserve-mib", "256"]
        self.run(command, timeout=90, monitored=action != "stop")

    def wait_node(self, node):
        deadline = time.monotonic() + 60
        while True:
            try:
                value = self.probe(node)
            except ValueError:
                self.foreign_nodes.add(node)
                raise
            except RuntimeError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(2)
                continue
            try:
                check_guest(value, node, enforce_resources=False)
            except ValueError:
                self.foreign_nodes.add(node)
                raise
            self.ready_nodes.add(node)
            self.remember_objects(node, value)
            check_guest(value, node)
            return

    def start(self):
        if any(self.vm_state().values()):
            raise ValueError("All three owned VMs must be fully stopped before configuring this profile")
        check_cold_start(self.host(), self.profile)
        for node in NODES:
            self.vm_command("configure", node)
        # Record intended ownership before a start command whose result can be unknown.
        self.owned_nodes.add("snow-analysis")
        self.started_nodes.add("snow-analysis")
        self.vm_command("start", "snow-analysis")
        self.wait_node("snow-analysis")
        state = self.probe("snow-analysis", "guard", monitored=True)
        if not state["has_input"]:
            self.vm_command("stop", "snow-analysis")
            self.ready_nodes.clear()
            self.owned_nodes.clear()
            self.started_nodes.clear()
            return dict(status="no_input", offline_services_started=False)
        for node in ("snow-control", "snow-compute"):
            self.owned_nodes.add(node)
            self.started_nodes.add(node)
            self.vm_command("start", node)
            self.wait_node(node)
        for node in ("snow-analysis", "snow-control", "snow-compute"):
            self.remote(node, "node-start-offline")
        return dict(status="offline_started", profile=self.profile, lifecycle_permit=False)

    def adopt_running(self):
        if not all(self.vm_state().values()):
            raise ValueError("Start the explicit offline profile first; phases never boot missing VMs")
        observed = {}
        for node in NODES:
            value = self.probe(node)
            check_guest(value, node, enforce_resources=False)
            self.remember_objects(node, value)
            observed[node] = value
            if self.vm.configured_memory(self.vm.RUNTIME / node / (node + ".vmx")) != self.memory[node]:
                raise ValueError("Running VM memory differs from the explicit small profile")
        self.probe("snow-analysis", "guard")
        self.ready_nodes.update(NODES)
        self.owned_nodes.update(NODES)
        # Establish actual ownership first so a subsequent low-resource result
        # can close these known objects instead of claiming an empty cleanup.
        for node, value in observed.items():
            check_guest(value, node)

    def stop(self):
        errors = []
        for node, attempt in tuple(self.inflight):
            try:
                self.run(self.ssh(node, "cancel", attempt=attempt), timeout=80, monitored=False)
                self.inflight.discard((node, attempt))
            except Exception:
                errors.append("cancel:" + node)
        for node in reversed(NODES):
            if node not in self.owned_nodes:
                continue
            if node in self.foreign_nodes:
                errors.append("foreign-work:" + node)
                continue
            try:
                # Stop is not admitted by RAM/disk gates, but foreign work is never adopted.
                value = self.probe(node)
                try:
                    check_guest(value, node, allow_driver=False, enforce_resources=False)
                    self.remember_objects(node, value)
                except ValueError:
                    self.foreign_nodes.add(node)
                    raise
                self.remote(node, "node-stop-offline")
                self.vm_command("stop", node)
                self.ready_nodes.discard(node)
            except Exception:
                # A VM started from the proven fully-off state may fail before
                # SSH is ready. Soft stop only that exact intended VM; never
                # apply this fallback to an adopted/pre-existing live node.
                if node in self.started_nodes and node not in self.ready_nodes and node not in self.foreign_nodes:
                    try:
                        self.vm_command("stop", node)
                        continue
                    except Exception:
                        pass
                errors.append("stop:" + node)
        if errors:
            raise RuntimeError("Some exact owned resources remain; inspect the retained receipt: " + ",".join(errors))
        return dict(status="offline_stopped", data_deleted=False)

    @contextmanager
    def monitored(self):
        self.monitor = Monitor(self.host, self.guests)
        try:
            self.monitor.start()
            yield
            self.monitor.check()
        finally:
            self.monitor.close()

    def perform(self, phase, run_id=None):
        if phase == "status":
            return self._perform(phase, run_id)
        folder = self.root / "runtime/real/offline-small/controller/shared-offline"
        if folder.resolve() != folder.absolute():
            raise ValueError("Controller ownership directory cannot traverse links")
        with publication_lock(folder):
            return self._perform(phase, run_id)

    def _perform(self, phase, run_id=None):
        if phase not in PHASES:
            raise ValueError("Unknown offline phase")
        if phase in JOB_PHASES:
            metadata_paths(self.config, run_id)
        if phase == "status":
            state = self.vm_state()
            return dict(status="observed", profile=self.profile, memory_mib=self.memory, vms=state, resources=self.host(),
                        guests={node: self.probe(node) for node, running in state.items() if running},
                        modifies_services=False)
        receipt = attempt_path(self.root, self.attempt) / "controller.json"
        report = dict(schema_version=1, source="real", phase=phase, run_id=run_id,
                      profile=self.profile, memory_mib=self.memory,
                      started_at=datetime.now(UTC).isoformat(), status="started", cleanup_complete=False)
        write_json(receipt, report)
        try:
            if phase == "stop-offline":
                # An explicit stop can adopt stopped/mixed VMs, but not unknown running workloads.
                accepted = set()
                for node, running in self.vm_state().items():
                    if running:
                        value = self.probe(node)
                        check_guest(value, node, enforce_resources=False)
                        self.remember_objects(node, value)
                        if self.vm.configured_memory(self.vm.RUNTIME / node / (node + ".vmx")) != self.memory[node]:
                            raise ValueError("Running VM memory differs from the explicit small profile")
                        accepted.add(node)
                self.owned_nodes.update(accepted)
                result = self.stop()
            else:
                # This is the operator's own registered copy namespace, never
                # a copied authority registry or an assertion about VM cleanup.
                cleanup_local_copies(self.root)
                if phase != "start-offline":
                    self.adopt_running()
                check_host(self.host(), 256)
                if phase != "start-offline":
                    state = self.probe("snow-analysis", "input")
                    needs_input = phase in {"prepare", "permit", "stage-compute", "daily", "behavior"}
                    if phase == "land" and not state["has_pending"] or needs_input and not state["has_input"]:
                        result = dict(status="no_input", phase=phase, metrics_fabricated=False)
                        report.update(status="no_input")
                        return result
                with self.monitored():
                    if phase == "start-offline":
                        result = self.start()
                    elif phase == "stage-compute":
                        self.stage_compute(run_id)
                        result = dict(status="metadata_staged")
                    elif phase == "stage-release":
                        self.stage_release(run_id)
                        result = dict(status="aggregate_pair_staged")
                    else:
                        result = self.remote(route(self.config, phase), phase, run_id)
            report.update(status=result["status"], cleanup_complete=True)
            return result
        except BaseException as error:
            report.update(status="failed", error_type=type(error).__name__)
            try:
                if self.monitor:
                    self.monitor.close()
                had_ownership = bool(self.owned_nodes or self.inflight)
                self.stop()
                report["cleanup_complete"] = had_ownership
                report["cleanup_scope"] = "owned_offline_nodes" if had_ownership else "none_admitted"
            except Exception as cleanup_error:
                report["cleanup_error_type"] = type(cleanup_error).__name__
            raise
        finally:
            report.update(finished_at=datetime.now(UTC).isoformat(),
                          monitor_samples=self.monitor.samples if self.monitor else 0,
                          last_host_sample=self.monitor.last if self.monitor else None,
                          owned_nodes=sorted(self.owned_nodes), data_deleted=False)
            write_json(receipt, report)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("phase", choices=PHASES)
    parser.add_argument("--run-id")
    parser.add_argument("--describe", action="store_true")
    parser.add_argument("--profile", choices=tuple(PROFILES), default=PROFILE,
                        help="Explicit profile, also required for later phases and stopping; default stays 1920")
    parser.add_argument("--node-operation", choices=("probe", "guard", "input", "execute", "cancel"), help=argparse.SUPPRESS)
    parser.add_argument("--node-phase", choices=sorted(REMOTE_PHASES), help=argparse.SUPPRESS)
    parser.add_argument("--attempt", help=argparse.SUPPRESS)
    parser.add_argument("--config-sha256", help=argparse.SUPPRESS)
    args = parser.parse_args()
    private_relative(args.config, "config", ".json")
    config = checked_config(read_json(ROOT / args.config))
    if args.phase in JOB_PHASES:
        metadata_paths(config, args.run_id)
    if args.describe:
        if args.node_operation:
            parser.error("Internal node operations are not controller descriptions")
        result = description(args.phase, args.run_id, args.profile)
    elif args.node_operation:
        if args.config_sha256 != digest(canonical(config)):
            raise ValueError("Node configuration differs from the controller's exact real scope")
        node = socket.gethostname()
        if os.name != "posix" or node not in NODES or ROOT != Path(REMOTE_ROOT):
            raise ValueError("Node operations execute only in an exact owned Linux checkout")
        if args.node_operation in {"guard", "input"} and node != "snow-analysis":
            raise ValueError("Only analysis owns real input and writer authority")
        if args.node_operation == "probe":
            result = guest_sample(ROOT, node, config, args.attempt)
        elif args.node_operation == "guard":
            result = paused_guard(config, ROOT)
        elif args.node_operation == "input":
            cleanup_local_copies(ROOT)
            result = input_state(config, ROOT)
        elif args.node_operation == "cancel":
            result = node_cancel(ROOT, config, args.attempt)
        else:
            if args.node_phase not in {"node-start-offline", "node-stop-offline"} and route(config, args.node_phase) != node:
                raise ValueError("Offline phase was sent to the wrong authority node")
            result = node_execute(config, args.config, ROOT, args.node_phase, args.run_id, args.attempt)
    else:
        if os.name != "nt":
            raise ValueError("Use the Windows controller for offline orchestration")
        result = SmallRunner(config, args.config, profile=args.profile).perform(args.phase, args.run_id)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
