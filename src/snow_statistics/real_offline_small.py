"""Finite Windows orchestration for the reviewed 2048/1920/768 MiB offline stage.

This module does not replace writer admission, lifecycle permits or job validation.
Its Linux entry points are fixed probes and supervised calls to the frozen runner.
"""
import argparse
import importlib.util
import json
import os
import re
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
MEMORY = {"snow-control": 2048, "snow-compute": 1920, "snow-analysis": 768}
PHASES = ("status", "start-offline", "stop-offline", "land", "prepare", "cleanup", "permit",
          "stage-compute", "daily", "behavior", "validate", "publish-private", "stage-release")
REMOTE_PHASES = set(PHASES) - {"status", "start-offline", "stop-offline", "stage-compute", "stage-release"}
REMOTE_PHASES |= {"node-start-offline", "node-stop-offline", "metadata-directories", "export-release",
                  "release-directories", "reserve-release", "import-release"}
MIB, GIB = 1024**2, 1024**3
STOP_BYTES = 255 * GIB // 4
LIMIT_BYTES = 64 * GIB
LOG_LIMIT = MIB


class NodeCommandFailure(RuntimeError):
    def __init__(self, code):
        self.returncode = code
        super().__init__("Owned local/SSH command failed")


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


def description(phase, run_id=None):
    if phase not in PHASES:
        raise ValueError("Unknown explicit offline phase")
    return dict(phase=phase, run_id=run_id, profile=PROFILE, memory_mib=MEMORY,
                start_reserve_mib=256, project_stop_bytes=STOP_BYTES, project_hard_bytes=LIMIT_BYTES,
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


def container_ownership(root, node, name, value, *, driver_id=None):
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
    actual = {(v["Type"], v["Name"] if v["Type"] == "volume" else v["Source"], v["Destination"], v["RW"]) for v in value["mounts"]}
    if (value["name"] != "/" + name or value["image"] != image or actual != mounts
            or not re.fullmatch(r"sha256:[a-f0-9]{64}", value["image_id"])):
        raise ValueError("Owned container image, name or mount identity changed")
    return digest(canonical({k: v for k, v in value.items() if k != "pid"}))


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
                                            driver_id=owned_driver(root, config, attempt) if name == "snow-spark-yarn" else None)
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
    try:
        if process_identity(identity["pid"]) == identity:
            os.kill(identity["pid"], signal.SIGTERM)
            until = time.monotonic() + 55
            while process_identity(identity["pid"]) == identity and time.monotonic() < until:
                time.sleep(0.25)
            if process_identity(identity["pid"]) == identity:
                raise RuntimeError("Remote worker cancellation has not completed")
    finally:
        driver_cleanup(root, worker["phase"], worker["run_id"])
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
        write_json(directory / "worker.json", dict(config_sha256=digest(canonical(config)), phase=phase,
                   run_id=run_id, process=process_identity(os.getpid())))
    closed, last_ping = threading.Event(), [time.monotonic()]
    def heartbeat():
        stream = stdin or sys.stdin.buffer
        while True:
            line = stream.readline(64)
            if line != b"ping\n":
                closed.set()
                return
            last_ping[0] = time.monotonic()
    threading.Thread(target=heartbeat, daemon=True).start()
    def interrupted(*_):
        raise InterruptedError("Offline worker was cancelled")
    previous = {sig: signal.signal(sig, interrupted) for sig in
                ([signal.SIGTERM, signal.SIGHUP] if hasattr(signal, "SIGHUP") else [signal.SIGTERM])}
    process = None
    log = directory / "private.log"
    drain_thread, log_failed = None, threading.Event()
    try:
        command = [sys.executable, "tools/real_lab.py", "--config", config_file, phase]
        if run_id:
            command += ["--run-id", run_id]
        with log.open("wb") as stream:
            process = subprocess.Popen(command, cwd=root, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT, start_new_session=True)
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
                if closed.is_set() or time.monotonic() - last_ping[0] > 20:
                    raise RuntimeError("Controller heartbeat was lost")
                if time.monotonic() >= deadline or log_failed.is_set():
                    raise RuntimeError("Offline phase exceeded its time or retained log bound")
                time.sleep(0.25)
            if process.returncode:
                raise RuntimeError("Frozen offline phase failed; inspect its private bounded log")
            drain_thread.join(timeout=5)
            if drain_thread.is_alive() or log_failed.is_set():
                raise RuntimeError("Frozen offline output exceeded its bounded metadata log")
        return phase_result(log, phase)
    finally:
        try:
            if process is not None:
                stop_tree(process)
        finally:
            try:
                driver_cleanup(root, phase, run_id)
            finally:
                if drain_thread:
                    drain_thread.join(timeout=10)
                for sig, handler in previous.items():
                    signal.signal(sig, handler)


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
    def __init__(self, config, config_file, root=ROOT):
        super().__init__(checked_config(config), config_file, root)
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
                "--config-sha256", digest(canonical(self.config)), "--node-operation", operation, "status"]
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
        try:
            while not finished.wait(0.5):
                if monitored and self.monitor:
                    self.monitor.check()
                if heartbeat:
                    process.stdin.write(b"ping\n")
                    process.stdin.flush()
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
                    except BrokenPipeError:
                        pass
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
            command += ["--profile", PROFILE]
        if action == "start":
            check_host(self.host(), MEMORY[node] + 256)
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
        return dict(status="offline_started", profile=PROFILE, lifecycle_permit=False)

    def adopt_running(self):
        if not all(self.vm_state().values()):
            raise ValueError("Start the explicit offline profile first; phases never boot missing VMs")
        observed = {}
        for node in NODES:
            value = self.probe(node)
            check_guest(value, node, enforce_resources=False)
            self.remember_objects(node, value)
            observed[node] = value
            if self.vm.configured_memory(self.vm.RUNTIME / node / (node + ".vmx")) != MEMORY[node]:
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
            return dict(status="observed", vms=state, resources=self.host(),
                        guests={node: self.probe(node) for node, running in state.items() if running},
                        modifies_services=False)
        receipt = attempt_path(self.root, self.attempt) / "controller.json"
        report = dict(schema_version=1, source="real", phase=phase, run_id=run_id,
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
        result = description(args.phase, args.run_id)
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
        result = SmallRunner(config, args.config).perform(args.phase, args.run_id)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
