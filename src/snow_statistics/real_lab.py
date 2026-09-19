"""Explicit on-demand phases; configuration cannot contain executable commands."""
import argparse
import importlib.util
import ipaddress
import json
import os
import re
import shlex
import socket
import stat
import subprocess
import sys
import time
from contextlib import contextmanager, nullcontext
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

from .io import atomic_write
from .landing import checked_receipt, collector_identity, load
from .lifecycle import RealLifecycle, timestamp

ROOT = Path(__file__).resolve().parents[2]
REMOTE_ROOT = "/home/snow/Snow_Statistics"
NODES = ("snow-control", "snow-compute", "snow-analysis")
PHASES = ("status", "start-offline", "stop-offline", "start-storage", "start-realtime", "stop-epoch",
          "initialize-writer", "submit-writer", "writer-status", "pause-writer", "resume-writer",
          "sync", "capture", "land", "ack", "prepare", "cleanup", "permit", "stage-compute", "stage-release",
          "daily", "behavior", "validate", "publish-private", "publish-doris", "view")
JOB_PHASES = {"prepare", "permit", "stage-compute", "stage-release", "daily", "behavior", "validate", "publish-private", "publish-doris",
              "export-release", "release-directories", "reserve-release", "import-release"}
TRANSPORT = {"sync", "capture", "land", "ack", "prepare", "cleanup", "permit"}
SERVICES = {"snow-control": ("namenode", "resourcemanager"), "snow-compute": ("datanode", "nodemanager"),
            "snow-analysis": ("datanode",)}


def private_relative(value, directory, suffix):
    if not isinstance(value, str) or not re.fullmatch(r"runtime/real/" + directory + r"/[A-Za-z0-9_-]{1,100}" + re.escape(suffix), value):
        raise ValueError("Use an exact file inside the dedicated ignored real namespace")
    return value


def validate_config(value):
    required = {"schema_version", "source", "input_origin", "lane", "transport_node", "nodes", "collector_url",
                "reader_token_file", "tunnel", "backend_config_file", "doris_config_file"}
    if set(value) != required or type(value["schema_version"]) is not int or value["schema_version"] != 1 or value["source"] != "real":
        raise ValueError("Unexpected real runner configuration")
    if not re.fullmatch(r"[a-z][a-z0-9-]{2,23}", value["lane"]) or value["transport_node"] not in {"snow-control", "snow-analysis"}:
        raise ValueError("Invalid owned transport lane/node")
    if value["input_origin"] not in {"real", "synthetic fixtures"}:
        raise ValueError("Declare the actual input origin")
    if value["input_origin"] == "real" and value["transport_node"] != "snow-analysis":
        raise ValueError("Real records require the independently owned analysis engine epoch")
    if value["input_origin"] == "synthetic fixtures" and not value["lane"].startswith("fixture-"):
        raise ValueError("Acceptance fixtures require an explicit fixture lane")
    if set(value["nodes"]) != set(NODES):
        raise ValueError("Configure exactly the three owned VMware nodes")
    for host in value["nodes"].values():
        ip = ipaddress.ip_address(host)
        if ip.version != 4 or not ip.is_private or ip.is_loopback:
            raise ValueError("Lab nodes must be explicit private IPv4 addresses")
    if len(set(value["nodes"].values())) != 3:
        raise ValueError("Lab nodes need distinct addresses")
    endpoint = urlsplit(value["collector_url"])
    if (endpoint.scheme != "http" or endpoint.hostname != "127.0.0.1" or endpoint.username or endpoint.password
            or endpoint.path not in {"", "/"} or endpoint.query or endpoint.fragment or not endpoint.port
            or not 1024 <= endpoint.port <= 65535):
        raise ValueError("Private collection reads must use an explicit loopback endpoint")
    private_relative(value["reader_token_file"], "secrets", ".token")
    for name in ("backend_config_file", "doris_config_file"):
        if value[name] is not None:
            private_relative(value[name], "config", ".json")
    tunnel = value["tunnel"]
    if value["input_origin"] == "synthetic fixtures" and tunnel is not None:
        raise ValueError("Fixture acceptance must use its own local collector without the production SSH tunnel")
    if tunnel is not None:
        if set(tunnel) != {"host", "user", "port", "key_file", "known_hosts_file", "remote_port"}:
            raise ValueError("Unexpected SSH tunnel fields")
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.-]{0,252}", tunnel["host"]) or not re.fullmatch(r"[a-z_][a-z0-9_-]{0,31}", tunnel["user"]):
            raise ValueError("Invalid SSH host/user")
        for name in ("port", "remote_port"):
            if type(tunnel[name]) is not int or not 1 <= tunnel[name] <= 65535:
                raise ValueError("Invalid SSH tunnel port")
        private_relative(tunnel["key_file"], "ssh", ".key")
        private_relative(tunnel["known_hosts_file"], "ssh", ".known_hosts")
    return value


def read_json(path, limit=65536):
    path = Path(path)
    if path.resolve() != path.absolute() or path.stat().st_size > limit:
        raise ValueError("Expected a bounded unlinked JSON file")
    return json.loads(path.read_bytes())


def secret_file(root, relative, limit=65536):
    path = root / relative
    info = path.stat()
    if (not stat.S_ISREG(info.st_mode) or path.resolve() != path.absolute() or info.st_size > limit
            or os.name == "posix" and info.st_mode & 0o077):
        raise ValueError("Private regular files must be unlinked, bounded and mode 0600")
    return path


def route(config, phase):
    if phase == "view":
        return config["transport_node"]
    if phase == "publish-doris" and config["input_origin"] == "real":
        return "snow-analysis"
    return config["transport_node"] if phase in TRANSPORT else "snow-analysis" if phase in {
        "start-storage", "start-realtime", "stop-epoch", "initialize-writer", "submit-writer", "writer-status", "pause-writer", "resume-writer",
        "release-directories", "reserve-release", "import-release"} else "snow-control"


def metadata_paths(config, run_id):
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", run_id or ""):
        raise ValueError("An explicit bounded run ID is required")
    return dict(job=f"runtime/real/jobs/{run_id}.json", coverage=f"runtime/real/coverage/{run_id}.json",
                permit=f"runtime/real/permits/{run_id}.json", receipt=f"runtime/real/permits/{run_id}.lifecycle.json",
                state=f"runtime/real/ods/{config['lane']}/state.json")


def compose(node, action):
    if node not in NODES or action not in {"up", "stop"}:
        raise ValueError("Only exact project offline services may be managed")
    role = node.removeprefix("snow-")
    command = ["sudo", "docker", "compose", "--env-file", "lab/locks/images.env", "--env-file", "lab/.env",
               "-f", f"lab/compose.{role}.yaml"]
    if node != "snow-analysis":
        command += ["-f", f"lab/compose.{role}-scale.yaml"]
    return command + (["up", "-d", "--no-build", "--no-deps"] if action == "up" else ["stop"]) + list(SERVICES[node])


def stage_nodes(config, stage):
    if stage == "offline":
        return NODES
    if stage != "storage":
        raise ValueError("Unknown VM resource stage")
    # Production transport is on analysis. Booting control as well reserves an
    # unnecessary 2 GiB of host RAM and VMware backing disk in this phase.
    return ("snow-analysis",) if config["input_origin"] == "real" else ("snow-control", "snow-analysis")


class Runner:
    def __init__(self, config, config_file, root=ROOT):
        self.config, self.config_file, self.root = config, config_file, Path(root)

    def run(self, command, *, body=None, output=None, timeout=1200, quiet=False, env=None):
        process = subprocess.Popen(command, cwd=self.root, stdin=subprocess.PIPE if body is not None else subprocess.DEVNULL,
                                   stdout=output if output else subprocess.DEVNULL if quiet else None,
                                   stderr=output if output else subprocess.DEVNULL if quiet else None, env=env)
        try:
            process.communicate(input=body, timeout=timeout)
            if process.returncode:
                raise RuntimeError("Owned phase failed; inspect retained progress privately")
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)

    def remote(self, node, phase, run_id=None):
        arguments = [".venv/bin/python", "tools/real_lab.py", "--config", self.config_file, phase]
        if run_id:
            arguments += ["--run-id", run_id]
        script = "set -euo pipefail\ncd " + REMOTE_ROOT + "\n" + shlex.join(arguments) + "\n"
        filename = self.root / "runtime/real/operator" / (node + "-" + phase + ".sh")
        atomic_write(filename, script.encode())
        self.run([sys.executable, "tools/lab_remote.py", "--node", node, "--script", str(filename)], timeout=1250)

    def windows_start(self, stage):
        selected = stage_nodes(self.config, stage)
        started = []
        try:
            for node in selected:
                profile = "realtime" if node == "snow-analysis" and stage != "offline" else "scale"
                self.run([sys.executable, "tools/vmware_lab.py", "configure", "--node", node, "--profile", profile])
                self.run([sys.executable, "tools/vmware_lab.py", "start", "--node", node])
                started.append(node)
            for node in selected:
                deadline = time.monotonic() + 60
                while True:
                    try:
                        self.remote(node, "status")
                        break
                    except RuntimeError:
                        if time.monotonic() >= deadline:
                            raise
                        time.sleep(2)
                if stage == "offline":
                    self.remote(node, "node-start-offline")
            if stage != "offline":
                self.remote("snow-analysis", "start-storage")
        except BaseException:
            for node in reversed(started):
                try:
                    self.run([sys.executable, "tools/vmware_lab.py", "stop", "--node", node])
                except Exception:
                    pass
            raise

    def stage_compute(self, run_id):
        paths = metadata_paths(self.config, run_id)
        local = self.root / "runtime/real/operator/metadata" / run_id
        local.mkdir(parents=True, exist_ok=True)
        for name, relative in paths.items():
            self.run([sys.executable, "tools/lab_remote.py", "--node", self.config["transport_node"],
                      "--download", str(local / (name + ".json")), "--remote", REMOTE_ROOT + "/" + relative])
        validate_transfer({key: read_json(local / (key + ".json"), 4 * 1024**2) for key in paths}, self.config, run_id)
        self.remote("snow-control", "metadata-directories", run_id)
        for name, relative in paths.items():
            self.run([sys.executable, "tools/lab_remote.py", "--node", "snow-control", "--upload", str(local / (name + ".json")),
                      "--remote", REMOTE_ROOT + "/" + relative])

    def stage_release(self, run_id):
        from .real_transfer import accept, cleanup_copies, paths, reserve, validate_metadata
        if self.config["input_origin"] != "real":
            raise ValueError("A production transfer cannot promote a synthetic fixture")
        relative = paths(self.config["lane"], run_id)
        directory = self.root / relative["directory"]
        if directory.resolve() != directory.absolute():
            raise ValueError("Aggregate transfer cannot traverse a link")
        directory.mkdir(parents=True, exist_ok=True)
        try:
            cleanup_copies(self.root)
            self.remote("snow-control", "export-release", run_id)
            self.run([sys.executable, "tools/lab_remote.py", "--node", "snow-control",
                      "--download", str(self.root / relative["incoming_manifest"]), "--remote", REMOTE_ROOT + "/" + relative["manifest"]])
            manifest = validate_metadata(read_json(self.root / relative["incoming_manifest"]), self.config, run_id)
            reserve(self.root, self.config, run_id, manifest)
            self.run([sys.executable, "tools/lab_remote.py", "--node", "snow-control",
                      "--download", str(self.root / relative["incoming_pair"]), "--remote", REMOTE_ROOT + "/" + relative["pair"]])
            accept(self.root, self.config, run_id)
            self.remote("snow-analysis", "release-directories", run_id)
            self.run([sys.executable, "tools/lab_remote.py", "--node", "snow-analysis",
                      "--upload", str(self.root / relative["manifest"]), "--remote", REMOTE_ROOT + "/" + relative["incoming_manifest"]])
            self.remote("snow-analysis", "reserve-release", run_id)
            self.run([sys.executable, "tools/lab_remote.py", "--node", "snow-analysis",
                      "--upload", str(self.root / relative["pair"]), "--remote", REMOTE_ROOT + "/" + relative["incoming_pair"]])
            self.remote("snow-analysis", "import-release", run_id)
        except BaseException:
            try:
                self.remote("snow-analysis", "stop-epoch")
            except Exception:
                pass
            raise


def validate_job(job, config, run_id):
    required = {"run_id", "kind", "source", "input", "warehouse_root", "auxiliary_root", "date_from", "date_to",
                "cutoff", "coverage_file", "auxiliary_file", "permit_file", "register_hive"}
    prefix = "hdfs://" + config["nodes"]["snow-control"] + ":9000/snow/"
    lane = config["lane"]
    if (set(job) != required or job["run_id"] != run_id or job["source"] != "real" or job["kind"] not in {"daily", "behavior"}
            or job["warehouse_root"] != prefix + "warehouse/real/" + lane
            or job["auxiliary_root"] != prefix + "auxiliary/real/" + lane
            or not re.fullmatch(re.escape(prefix + "ods/real/kafka/" + lane) + r"/snapshots/[a-f0-9]{64}/_snapshot.json", job["input"])
            or job["coverage_file"] != run_id + ".json" or job["permit_file"] != run_id + ".json"
            or job["register_hive"] is not False or job["auxiliary_file"] is not None):
        raise ValueError("Initial runner accepts exact event-only jobs; auxiliary continuation requires its registered operator path")
    from .scheduling import resolve_window
    resolve_window(job["cutoff"], run_id, job["date_from"], job["date_to"], job["cutoff"])
    return job


def validate_transfer(values, config, run_id):
    from .real_behavior import validate_coverage
    from .real_remote_lifecycle import outputs_for
    job = validate_job(values["job"], config, run_id)
    state = values["state"]
    if set(state) != {"schema_version", "source", "identity", "offsets", "batches", "root", "head_batch_id", "snapshot_id", "batch_id", "input"}:
        raise ValueError("Unexpected ODS metadata fields")
    snapshot = checked_receipt(state)
    coverage = validate_coverage(values["coverage"], job["cutoff"], snapshot["identity"]["collector"])
    permit = values["permit"]
    if (set(permit) != {"schema_version", "source", "input_snapshot", "coverage_sha256", "auxiliary_sha256", "outputs",
                        "hive_tables", "issued_at", "expires_at", "lifecycle_receipt_sha256"}
            or state["input"] != job["input"] or permit["input_snapshot"] != job["input"]
            or permit["outputs"] != outputs_for(job) or permit["source"] != "real"
            or not timestamp(permit["issued_at"]) <= datetime.now(UTC) < timestamp(permit["expires_at"])):
        raise ValueError("Transferred permit does not match a live exact job")
    if values["receipt"]["owner"] != {key: coverage[key] for key in ("instance_id", "generation")}:
        raise ValueError("Transferred source generation differs")
    return job


@contextmanager
def tunnel(config, root):
    value = config["tunnel"]
    if value is None:
        yield
        return
    local_port = urlsplit(config["collector_url"]).port
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", local_port))
    key = secret_file(root, value["key_file"], 16384)
    known = secret_file(root, value["known_hosts_file"], 65536)
    command = ["ssh", "-N", "-T", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
               "-o", "HostKeyAlias=snow-statistics-collector", "-o", "UserKnownHostsFile=" + str(known),
               "-o", "ExitOnForwardFailure=yes", "-o", "ConnectTimeout=10", "-o", "ServerAliveInterval=15",
               "-o", "ServerAliveCountMax=2", "-i", str(key), "-p", str(value["port"]),
               "-L", f"127.0.0.1:{local_port}:127.0.0.1:{value['remote_port']}", value["user"] + "@" + value["host"]]
    process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        deadline = time.monotonic() + 15
        while True:
            if process.poll() is not None:
                raise RuntimeError("Controlled SSH tunnel could not start")
            try:
                connection = socket.create_connection(("127.0.0.1", local_port), timeout=0.3)
                connection.close()
                break
            except OSError:
                if time.monotonic() >= deadline:
                    raise TimeoutError("Controlled SSH tunnel did not become available")
                time.sleep(0.1)
        yield
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)


def no_input():
    return {"source": "real", "status": "no_computable_input", "message": "暂无可计算输入；未生成零值报表。"}


def job_data(config, root, run_id):
    paths = metadata_paths(config, run_id)
    return validate_job(read_json(root / paths["job"]), config, run_id), paths


@contextmanager
def hdfs_context(config):
    from .hdfs_landing import HdfsSink
    from .real_remote_lifecycle import WebHdfsOwned
    sink = HdfsSink(config["nodes"]["snow-control"], config["lane"],
                    {key: config["nodes"][key] for key in ("snow-compute", "snow-analysis")}, source="real")
    try:
        yield sink, WebHdfsOwned(sink)
    finally:
        sink.client.close()


def lifecycle_phase(config, root, phase, run_id):
    from .real_backend_lifecycle import backend_adapters
    from .real_remote_lifecycle import RealRemoteLifecycle
    manager = RealRemoteLifecycle(root / "runtime/real/lifecycle" / config["lane"])
    if config["input_origin"] == "real" and (root / "runtime/real/epochs").exists():
        from .real_epoch import expire_due
        from .real_quiescent import DockerStorage
        expire_due(root / "runtime/real/epochs", DockerStorage())
    ods = root / "runtime/real/ods" / config["lane"]
    state = load(ods / "state.json")
    if state is None:
        return no_input()
    snapshot = checked_receipt(state)
    if phase != "cleanup" and (not snapshot["batches"] or not sum(entry["counts"]["events"] for entry in snapshot["batches"])):
        return no_input()
    job, paths = job_data(config, root, run_id) if phase in {"prepare", "permit"} else (None, None)
    if job and state["input"] != job["input"]:
        raise ValueError("Job must bind the current committed ODS snapshot")
    registry, retired = None, None
    if config["input_origin"] == "real":
        from .real_quiescent import backend_resources, bind_ods
        registry = writer_registry(config, root)
        if timestamp(registry.epoch.read()["expires_at"]) <= datetime.now(UTC):
            from .real_retired import RetiredStorage
            retired = RetiredStorage(registry.epoch, snapshot)
            registration = retired.registration()
            if phase == "prepare":
                raise ValueError("A retired engine epoch cannot reserve new raw compute outputs")
        else:
            registration = registry.ready(snapshot["identity"]["collector"])
        bind_ods(registration, snapshot)
    if phase == "prepare":
        identity = snapshot["identity"]["collector"]
        if not manager.owner.exists():
            manager.initialize(job["warehouse_root"], job["auxiliary_root"], snapshot["root"], identity["instance_id"], identity["generation"])
        owner, registered = manager._read()
        if any(owner[key] != identity[key] for key in ("instance_id", "generation")):
            raise ValueError("A different collector generation requires a new owned lane")
        original = min((entry["original_min_accepted_at"] for entry in snapshot["batches"]), key=timestamp)
        # Capture proves this backend has held rows: it must never remain marked
        # not_initialized merely because an operator omitted backend config.
        if registry:
            for backend, resources in backend_resources(registration).items():
                for resource, entry in resources.items():
                    manager.register_backend(backend, resource, entry["kind"], entry["original_min_accepted_at"])
        else:
            for topic in snapshot["identity"]["topic_ids"]:
                existing = registered["backends"]["kafka"]["resources"].get(topic)
                if existing is None:
                    manager.register_backend("kafka", topic, "raw", original)
                elif existing["kind"] != "raw" or timestamp(existing["original_min_accepted_at"]) > timestamp(original):
                    raise ValueError("Kafka input predates its original registered lifetime")
        manager.reserve_job(job, original, original)
        return {"source": "real", "reserved": True, "run_id": run_id, "read_permission": False}
    with hdfs_context(config) as (sink, hdfs):
        filename = config["backend_config_file"]
        adapters = backend_adapters(secret_file(root, filename), hdfs) if filename else nullcontext({})
        if retired:
            adapters = nullcontext(retired.adapters())
        elif registry:
            from .real_quiescent import StoppedStorage
            containers, _ = registry.epoch._inspect(registry.epoch.read())
            if containers and all(not value["running"] for value in containers.values()):
                # This has its own receipt type. A stopped backend is never
                # recorded as uninitialized or as having passed online SQL.
                adapters = nullcontext(StoppedStorage(registry, snapshot).adapters())
        with adapters as checks:
            _, retained = manager._read()
            if retained["backends"]["hive"]["state"] == "initialized" or (manager.directory / "hive-catalog.json").exists():
                from .real_hive import HiveRegistry, HiveRetention, SparkCatalog
                catalog_registry = HiveRegistry(manager)
                checks["hive"] = HiveRetention(catalog_registry, SparkCatalog(root, catalog_registry, config["nodes"]["snow-control"]))
            if phase == "cleanup":
                return manager.cleanup(hdfs, ods, sink, backend_checks=checks)
            return manager.issue_permit(job, root / paths["coverage"], None, root / paths["permit"], hdfs, ods, sink, backend_checks=checks)


def writer_registry(config, root):
    from .real_epoch import Epoch, expire_due
    from .real_quiescent import DockerStorage, WriterRegistry
    docker = DockerStorage()
    directory = root / "runtime/real/epochs"
    expire_due(directory, docker)
    return WriterRegistry(Epoch(directory / config["lane"], docker))


def check_epoch(config, root, role):
    if config["transport_node"] == "snow-control" and config["input_origin"] == "synthetic fixtures":
        return  # This explicitly labelled fixture uses the pre-existing isolated test broker.
    from .real_epoch import DockerEpoch, Epoch, expire_due
    directory = root / "runtime/real/epochs"
    docker = DockerEpoch()
    expire_due(directory, docker)
    epoch = Epoch(directory / config["lane"], docker)
    manifest = epoch.read()
    if timestamp(manifest["expires_at"]) <= datetime.now(UTC):
        raise ValueError("Expired real transport epoch")
    containers, _ = epoch._inspect(manifest)
    container = manifest["containers"][role]
    if container not in containers or not containers[container]["running"]:
        raise ValueError("Start the independently owned transport storage stage first")
    if config["input_origin"] == "real":
        from .real_backend_lifecycle import KafkaClient
        from .real_quiescent import storage
        registry = writer_registry(config, root)
        registered = registry.ready()
        if storage(registry.epoch, running=None) != registered["storage"]:
            raise ValueError("Real writer storage changed after initialization")
        if role == "kafka":
            client = KafkaClient(config["nodes"]["snow-analysis"] + ":9092", manifest["containers"]["kafka"])
            try:
                expected = registered["initial"]["kafka"]["identity"]
                if client.identity(expected["topic_ids"]) != expected:
                    raise ValueError("Real Kafka cluster/topic identity changed before writing or capture")
            finally:
                client.close()
        return registry


def execute_linux(runner, phase, run_id=None):
    config, root = runner.config, runner.root
    node = socket.gethostname()
    if node not in NODES or root.as_posix() != REMOTE_ROOT:
        raise ValueError("Execute Linux phases only in the owned VMware checkout")
    if phase == "status":
        directory = root / "runtime/real/sync" / config["lane"]
        return dict(source="real", node=node, input_origin=config["input_origin"], lane=config["lane"],
                    cursor=load(directory / "cursor.json", {"cursor": None})["cursor"],
                    unresolved_gap=(directory / "gap.json").exists(), online_products_dependency=False)
    if phase in {"node-start-offline", "node-stop-offline"}:
        action = "up" if phase == "node-start-offline" else "stop"
        if action == "up":
            from .real_transfer import cleanup_copies
            cleanup_copies(root)
        if action == "up" and node == "snow-analysis":
            from .real_epoch import DockerEpoch, expire_due, stop_all
            from .real_quiescent import WriterRegistry
            epoch_root = root / "runtime/real/epochs"
            if epoch_root.exists():
                docker = DockerEpoch()
                expire_due(epoch_root, docker)
                from .real_epoch import Epoch
                for folder in epoch_root.iterdir():
                    if folder.is_dir() and (folder / "writer-job.json").exists() and not (folder / "retirement.json").exists():
                        history = WriterRegistry(Epoch(folder, docker)).recovery()
                        if not history or history[-1]["action"] != "paused":
                            raise ValueError("Pause the registered real writer before starting offline services")
                stop_all(epoch_root, docker)
        try:
            runner.run(compose(node, action))
        except BaseException:
            if action == "up":
                runner.run(compose(node, "stop"), quiet=True)
            raise
        return {"node": node, "offline_services": action, "data_deleted_by_stop": False}
    if phase == "metadata-directories":
        for relative in metadata_paths(config, run_id).values():
            (root / relative).parent.mkdir(parents=True, exist_ok=True)
        return {"metadata_directories_ready": True}
    if route(config, phase) != node:
        raise ValueError("This phase belongs to a different explicitly configured VM")
    if phase in {"export-release", "release-directories", "reserve-release", "import-release"}:
        if config["input_origin"] != "real":
            raise ValueError("Production transfer cannot adopt a synthetic fixture")
        from .real_transfer import accept, export, paths, reserve
        relative = paths(config["lane"], run_id)
        directory = root / relative["directory"]
        try:
            if phase == "release-directories":
                if directory.resolve() != directory.absolute():
                    raise ValueError("Transfer directories cannot traverse links")
                directory.mkdir(parents=True, exist_ok=True)
            elif phase == "export-release":
                export(root, config, run_id)
            elif phase == "reserve-release":
                reserve(root, config, run_id, read_json(root / relative["incoming_manifest"]))
            else:
                accept(root, config, run_id, publish=True)
        except BaseException:
            if node == "snow-analysis":
                from .real_epoch import Epoch
                from .real_quiescent import DockerStorage
                Epoch(root / "runtime/real/epochs" / config["lane"], DockerStorage()).stop()
            raise
        return {"source": "real", "phase": phase, "run_id": run_id, "aggregate_only": True}
    if phase == "publish-doris" and config["input_origin"] == "real":
        from .real_epoch import Epoch
        from .real_publication import read_real_release
        from .real_quiescent import DockerStorage
        from .real_transfer import paths
        epoch = Epoch(root / "runtime/real/epochs" / config["lane"], DockerStorage())
        try:
            relative = paths(config["lane"], run_id)
            directory = root / relative["published"]
            release = read_real_release(directory)
            if release["run_id"] != run_id:
                raise ValueError("Publish only the explicitly transferred managed release")
            with tunnel(config, root):
                runner.run([sys.executable, "tools/real_writer.py", "--root", str(root / "runtime/real/epochs"),
                            "--epoch", config["lane"], "--collector-url", config["collector_url"],
                            "--reader-token-file", str(secret_file(root, config["reader_token_file"], 4096)),
                            "--release-directory", str(directory), "publish"], timeout=1200)
        except BaseException:
            epoch.stop()
            raise
        return {"source": "real", "run_id": run_id, "doris_published": True, "execution_node": "snow-analysis"}
    if phase in {"initialize-writer", "submit-writer", "writer-status", "pause-writer", "resume-writer"}:
        if config["input_origin"] != "real":
            raise ValueError("Production writer phases cannot promote synthetic fixtures")
        from .real_writer import ActualWriter
        writer = None
        with tunnel(config, root):
            try:
                writer = ActualWriter(root / "runtime/real/epochs", config["lane"], config["collector_url"],
                                      secret_file(root, config["reader_token_file"], 4096))
                if phase == "initialize-writer":
                    writer.initialize()
                elif phase == "submit-writer":
                    writer.submit()
                elif phase in {"pause-writer", "resume-writer"}:
                    from .real_writer_recovery import WriterRecovery
                    recovery = WriterRecovery(writer)
                    recovery.pause() if phase == "pause-writer" else recovery.resume()
                else:
                    writer.registry.ready(writer.identity())
                    writer.registry.recovery()
            except BaseException:
                # Recovery owns failure stops: lock contention must never stop
                # an existing producer that still owns the operation lease.
                if writer is not None and phase not in {"writer-status", "pause-writer", "resume-writer"}:
                    writer.epoch.stop()
                raise
        return {"source": "real", "phase": phase, "epoch_id": config["lane"], "writer_registration_validated": True}
    if phase in {"start-storage", "start-realtime", "stop-epoch"}:
        if phase != "stop-epoch":
            from .real_epoch import expire_due
            from .real_quiescent import DockerStorage
            expire_due(root / "runtime/real/epochs", DockerStorage())
        command = [sys.executable, "tools/real_epoch.py", "--root", str(root / "runtime/real/epochs")]
        command += ["stop" if phase == "stop-epoch" else "start", "--epoch", config["lane"]]
        if phase != "stop-epoch":
            command += ["--stage", "storage" if phase == "start-storage" else "realtime"]
        runner.run(command)
        return {"phase": phase, "data_deleted_by_stop": False}
    if phase == "sync":
        registry = check_epoch(config, root, "kafka")
        token = secret_file(root, config["reader_token_file"], 4096).read_text().strip()
        if not re.fullmatch(r"[A-Za-z0-9_-]{16,1024}", token):
            raise ValueError("Invalid private reader token file")
        with registry.operation_lock() if registry else nullcontext():
            return sync_phase(config, root, registry, token)
    if phase in {"capture", "ack"}:
        from .kafka_landing import KafkaSource
        from .landing import acknowledge, capture
        registry = check_epoch(config, root, "kafka")
        identity = collector_identity(read_json(root / "runtime/real/sync" / config["lane"] / "source.json"))
        if registry:
            registry.ready(identity)
        broker = KafkaSource(config["nodes"][node] + ":9092", "snow-ods-" + config["lane"], source="real",
                             event_lane=config["lane"].replace("-", "_"), collector_identity=identity,
                             kafka_container="snow-real-" + config["lane"] + "-kafka" if node == "snow-analysis" else None)
        try:
            directory = root / "runtime/real/ods" / config["lane"]
            if phase == "ack":
                return acknowledge(directory, broker)
            batch = capture(directory, broker, max_records=10000, provenance="real", event_lane=config["lane"].replace("-", "_"))
            return {"source": "real", "batch_id": batch, "kafka_acked": False} if batch else no_input()
        finally:
            broker.close()
    if phase == "land":
        from .landing import land
        directory = root / "runtime/real/ods" / config["lane"]
        if not (directory / "pending.json").exists():
            return no_input()
        with hdfs_context(config) as (sink, _):
            return land(directory, sink)
    if phase in {"prepare", "cleanup", "permit"}:
        return lifecycle_phase(config, root, phase, run_id)
    if phase in {"daily", "behavior", "validate", "publish-private", "publish-doris"}:
        return model_phase(runner, phase, run_id)
    if phase == "view":
        metadata_paths(config, run_id)
        runner.run([sys.executable, "-m", "streamlit", "run", "dashboard/real_app.py", "--server.address=127.0.0.1",
                    "--server.port=8502", "--server.headless=true"], timeout=43200,
                   env=os.environ | {"SNOW_REAL_VIEW_CONFIG": runner.config_file, "SNOW_REAL_VIEW_RUN_ID": run_id})
        return {"view_stopped": True}
    raise ValueError("Use Windows for VM orchestration or an explicit Linux phase")


def sync_phase(config, root, registry, token):
    """Caller owns the full epoch operation lease until producer.close()."""
    from .sync import kafka_sync
    with tunnel(config, root):
        if registry:
            from .real_writer import ActualWriter
            from .real_writer_recovery import WriterRecovery
            writer = ActualWriter(root / "runtime/real/epochs", config["lane"], config["collector_url"],
                                  secret_file(root, config["reader_token_file"], 4096))
            registry.sync_ready(writer.identity())
            recovery = WriterRecovery(writer)
            recovery.guard()
            job = registry.current_job()
            recovery.inventory(recovery.known_jobs(registry.recovery()), [job])
            if recovery.probe.job(job)["state"] != "RUNNING":
                raise ValueError("Synchronize only while the registered actual writer is RUNNING")
        from .real_quiescent import WindowGuard
        source_file = root / "runtime/real/sync" / config["lane"] / "source.json"
        count = kafka_sync(config["collector_url"], token, config["nodes"][config["transport_node"]] + ":9092",
                           root / "runtime/real/sync" / config["lane"], lane=config["lane"].replace("-", "_"), source="real",
                           before_publish=WindowGuard(registry, source_file) if registry else None)
    return {"source": "real", "accepted_by_kafka": count, "status": "synced" if count else "no_new_input"}


def model_phase(runner, phase, run_id):
    from .real_publication import release_real, validate_real_pair
    config, root = runner.config, runner.root
    job, paths = job_data(config, root, run_id)
    local = RealLifecycle(root / "runtime/real/runs" / run_id / "data")
    if phase in {"daily", "behavior"}:
        spec = importlib.util.spec_from_file_location("real_job_gateway", root / "tools/airflow_real_gateway.py")
        gateway = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(gateway)
        command = gateway.build_command(job | {"kind": phase}, root=root)
        state = checked_receipt(read_json(root / paths["state"], 4 * 1024**2))
        if not state["batches"]:
            return no_input()
        original = min((entry["original_min_accepted_at"] for entry in state["batches"]), key=timestamp)
        from .real_behavior import HK
        aggregate_origin = datetime.fromisoformat(job["date_from"]).replace(tzinfo=HK).isoformat()
        if not local.owner.exists():
            local.initialize()
        local.cleanup()
        package = local.register(phase + ".json", "aggregate", aggregate_origin)
        local.register(phase + ".tmp", "aggregate", aggregate_origin)
        log = local.register(phase + ".log", "raw", original)
        cid = local.register(phase + ".cid", "raw", original)
        local.cleanup()
        if package.exists() or log.exists() or cid.exists():
            raise ValueError("Use a fresh run ID after an interrupted immutable compute attempt")
        command[1] = "tools/spark_yarn_scale.sh"
        command[2:2] = ["--conf", "spark.eventLog.enabled=false"]
        command[command.index("--package-file") + 1] = "/opt/snow/" + package.relative_to(root).as_posix()
        from .real_lineage import RealCapture, RealJournal, dataset
        from .real_publication import validate_manifest, validate_real_model
        with RealCapture(RealJournal(root / "runtime/real/lineage/events.sqlite"), "snow_real.compute_" + phase,
                         run_id, [dataset(job["input"])]) as capture, log.open("wb") as stream:
            try:
                runner.run(command, output=stream, timeout=1100, env=os.environ | {"SNOW_REAL_DRIVER_CIDFILE": str(cid)})
            finally:
                if cid.exists():
                    stop_driver(runner, cid)
            result = read_json(local.readable(phase + ".json"), 4 * 1024**2)
            if phase == "daily":
                from .publication import validate
                validate(result)
                validate_manifest(result["manifest"], "daily")
            else:
                validate_real_model(result)
            output = result["manifest"]["output"]
            capture.outputs = [dataset(output + "/" + name) for name in (("ads_daily",) if phase == "daily" else ("session_daily", "retention", "funnel"))]
        return {"source": "real", "run_id": run_id, "computed": phase, "package_registered": True,
                "application_id": result["manifest"]["application_id"]}
    local.cleanup()
    daily = read_json(local.readable("daily.json"), 4 * 1024**2)
    behavior = read_json(local.readable("behavior.json"), 4 * 1024**2)
    validate_real_pair(daily, behavior)
    if phase == "validate":
        return {"source": "real", "run_id": run_id, "same_pair_validated": True}
    if phase == "publish-private":
        result = release_real(daily, behavior, root / "runtime/real/publication", run_id)
        return {"source": "real", "run_id": run_id, "content_hash": result["content_hash"]}
    if config["input_origin"] == "real":
        raise ValueError("Production Doris publication requires the analysis-local registered writer; direct control-node SQL is disabled")
    if config["doris_config_file"] is None:
        raise ValueError("Doris requires an explicitly configured private real account")
    connection = read_json(secret_file(root, config["doris_config_file"]))
    if (set(connection) != {"host", "user", "password", "database"} or connection["host"] != config["nodes"]["snow-analysis"]
            or connection["database"] != "snow_real_" + config["lane"].replace("-", "_")
            or not re.fullmatch(r"snow_real_[a-z0-9_]{1,40}", connection["user"])):
        raise ValueError("Doris credentials must name only this isolated real epoch/database")
    import pymysql

    from .publication import publish
    prior = os.environ.get("SNOW_DORIS_DATABASE")
    os.environ["SNOW_DORIS_DATABASE"] = connection["database"]
    try:
        with pymysql.connect(host=connection["host"], port=9030, user=connection["user"], password=connection["password"],
                             autocommit=True, connect_timeout=10, read_timeout=60, write_timeout=60) as db:
            result = publish(db, daily, root / "runtime/real/publication")
    finally:
        if prior is None:
            os.environ.pop("SNOW_DORIS_DATABASE", None)
        else:
            os.environ["SNOW_DORIS_DATABASE"] = prior
    return {"source": "real", "run_id": run_id, "doris_snapshot": result["snapshot_id"]}


def stop_driver(runner, cid):
    identifier = cid.read_text().strip()
    if not re.fullmatch(r"[a-f0-9]{64}", identifier):
        raise ValueError("Invalid container identity from the owned run")
    result = subprocess.run(["sudo", "docker", "ps", "--no-trunc", "--filter", "id=" + identifier, "--format", "{{.ID}}"],
                            capture_output=True, text=True, check=True, timeout=10)
    if result.stdout.strip() == identifier:
        runner.run(["sudo", "docker", "stop", "-t", "10", identifier], quiet=True, timeout=30)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, help="Ignored runtime/real/config/<name>.json; no shell fields")
    parser.add_argument("phase", choices=PHASES + ("node-start-offline", "node-stop-offline", "metadata-directories",
                                                "export-release", "release-directories", "reserve-release", "import-release"))
    parser.add_argument("--run-id", help="Exact registered job ID")
    parser.add_argument("--power-off", action="store_true", help="stop-offline/stop-epoch only: soft-stop owned VMs after services")
    parser.add_argument("--describe", action="store_true", help="Validate routing only; execute nothing")
    args = parser.parse_args()
    private_relative(args.config, "config", ".json")
    config = validate_config(read_json(ROOT / args.config))
    if args.phase in JOB_PHASES or args.phase == "metadata-directories":
        metadata_paths(config, args.run_id)
    if args.power_off and args.phase not in {"stop-offline", "stop-epoch"}:
        parser.error("--power-off is only valid for stop-offline or stop-epoch")
    if args.describe:
        print(json.dumps(dict(phase=args.phase, execution_node=route(config, args.phase), source="real",
                              input_origin=config["input_origin"], run_id=args.run_id), ensure_ascii=False))
        return
    runner = Runner(config, args.config)
    try:
        if os.name != "nt":
            result = execute_linux(runner, args.phase, args.run_id)
        elif args.phase == "status":
            runner.run([sys.executable, "tools/vmware_lab.py", "status"])
            result = {"source": "real", "runtime_status": "local VM status only; inspect Linux status for data cutoff"}
        elif args.phase in {"start-offline", "start-storage"}:
            runner.windows_start("offline" if args.phase == "start-offline" else "storage")
            result = {"phase": args.phase, "started": True}
        elif args.phase == "stop-offline":
            errors = []
            for node in reversed(NODES):
                try:
                    runner.remote(node, "node-stop-offline")
                except Exception:
                    errors.append(node)
            if args.power_off:
                for node in reversed(NODES):
                    try:
                        runner.run([sys.executable, "tools/vmware_lab.py", "stop", "--node", node])
                    except Exception:
                        errors.append(node)
            if errors:
                raise RuntimeError("Some owned nodes could not be stopped; inspect each recorded VM")
            result = {"stopped": True, "data_deleted": False}
        elif args.phase == "stage-compute":
            runner.stage_compute(args.run_id)
            result = {"metadata_staged": True, "run_id": args.run_id}
        elif args.phase == "stage-release":
            runner.stage_release(args.run_id)
            result = {"aggregate_pair_staged": True, "run_id": args.run_id}
        elif args.phase == "stop-epoch":
            runner.remote("snow-analysis", "stop-epoch")
            if args.power_off:
                for node in reversed(stage_nodes(config, "storage")):
                    runner.run([sys.executable, "tools/vmware_lab.py", "stop", "--node", node])
            result = {"stopped": True, "data_deleted": False}
        else:
            runner.remote(route(config, args.phase), args.phase, args.run_id)
            result = {"phase": args.phase, "completed": True}
        print(json.dumps(result, ensure_ascii=False))
    except Exception as error:
        print(json.dumps(dict(source="real", phase=args.phase, status="stopped_with_progress_retained",
                              error_type=type(error).__name__,
                              next_action="Inspect private state; stopped registered backends still require an actual lifecycle check before permit")), file=sys.stderr)
        raise SystemExit(1) from None
