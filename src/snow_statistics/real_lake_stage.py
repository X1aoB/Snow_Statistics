"""Exact existing-container phase switching; NN/DN identities remain untouched."""
import json
import socket
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from urllib.request import urlopen

from .io import digest, write_json
from .publication import canonical, publication_lock
from .real_lab import validate_config
from .real_lake_authority import location, read

SERVICES = {
    "snow-control": {"namenode": 512, "hive": 1024, "resourcemanager": 384},
    "snow-compute": {"datanode": 384, "nodemanager": 1536},
    "snow-analysis": {"datanode": 768},
}
BASELINE = {"snow-control": {"namenode", "hive"}, "snow-compute": {"datanode"}, "snow-analysis": {"datanode"}}
YARN = {"snow-control": {"namenode", "resourcemanager"}, "snow-compute": {"datanode", "nodemanager"}, "snow-analysis": {"datanode"}}


def container_hash(value):
    return digest(canonical({key: value[key] for key in ("Id", "Name", "Image", "Config", "HostConfig", "Mounts")}))


class StageDocker:
    def __init__(self, root, config):
        self.root = Path(root)
        self.nodes = dict(validate_config(config)["nodes"])

    def run(self, *arguments, timeout=20):
        return subprocess.run(["sudo", "docker", *arguments], capture_output=True, check=True, timeout=timeout).stdout

    def snapshot(self, node):
        pins = dict(line.split("=", 1) for line in (self.root / "lab/locks/images.env").read_text().splitlines()
                    if line.startswith(("HADOOP_IMAGE=", "HIVE_IMAGE=")))
        names = {service: "snow-lab-" + node.removeprefix("snow-") + "-" + service + "-1" for service in SERVICES[node]}
        values = json.loads(self.run("inspect", *names.values()))
        result = {}
        for value in values:
            service = next((service for service, name in names.items() if value["Name"] == "/" + name), None)
            labels = value["Config"].get("Labels") or {}
            image = "snow-yarn-spark:0.1.0" if service == "nodemanager" else pins["HIVE_IMAGE" if service == "hive" else "HADOOP_IMAGE"]
            if (service is None or labels.get("com.docker.compose.project") != "snow-lab-" + node.removeprefix("snow-")
                    or labels.get("com.docker.compose.service") != service
                    or value["Config"]["Image"] != image
                    or value["HostConfig"]["Memory"] != SERVICES[node][service] * 1024**2
                    or value["HostConfig"]["NetworkMode"] != "host" or value["State"]["OOMKilled"]):
                raise ValueError("Existing lab container ownership, limits, network or OOM state differs")
            result[service] = dict(id=value["Id"], identity_sha256=container_hash(value),
                                   running=value["State"]["Running"], started_at=value["State"]["StartedAt"],
                                   restarts=value["RestartCount"])
        if set(result) != set(names):
            raise ValueError("Every stage service must already exist; no create/recreate is permitted")
        running = self.run("ps", "--format", "{{.Names}}").decode().splitlines()
        if set(running) != {names[key] for key, entry in result.items() if entry["running"]}:
            raise ValueError("Unrelated running containers block the exclusive lake phase")
        return result

    def stop_owned(self, entry):
        value = json.loads(self.run("inspect", entry["id"]))
        if len(value) != 1 or value[0]["Id"] != entry["id"] or container_hash(value[0]) != entry["identity_sha256"]:
            raise ValueError("Refusing recovery of a changed stage container")
        if value[0]["State"]["Running"]:
            self.change(entry["id"], "stop")
        value = json.loads(self.run("inspect", entry["id"]))
        if len(value) != 1 or value[0]["State"]["Running"]:
            raise ValueError("Owned stage container remains running after recovery stop")

    def change(self, container_id, action):
        if action == "stop":
            self.run("stop", "-t", "20", container_id, timeout=35)
        elif action == "start":
            self.run("start", container_id, timeout=35)
        else:
            raise ValueError("Only exact existing container start/stop is allowed")

    def memory(self):
        values = {line.split(":", 1)[0]: int(line.split()[1]) for line in Path("/proc/meminfo").read_text().splitlines()}
        return values["MemTotal"], values["MemAvailable"]

    def ready(self, node, service):
        if (node, service) not in {("snow-control", "hive"), ("snow-control", "resourcemanager"),
                                   ("snow-compute", "nodemanager")}:
            raise ValueError("Readiness is restricted to the fixed node/service endpoints")
        host = self.nodes[node]
        if service == "hive":
            with socket.create_connection((host, 9083), timeout=2):
                return True
        port = 8088 if service == "resourcemanager" else 8042
        path = "/ws/v1/cluster/info" if service == "resourcemanager" else "/ws/v1/node/info"
        with urlopen("http://" + host + ":" + str(port) + path, timeout=2) as response:
            value = json.loads(response.read(65536))
        return (value.get("clusterInfo", {}).get("state") == "STARTED" if service == "resourcemanager" else
                value.get("nodeInfo", {}).get("nodeHealthy") is True)

    def hdfs_ready(self):
        endpoint = "http://" + self.nodes["snow-control"] + ":9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState"
        with urlopen(endpoint, timeout=3) as response:
            value = json.loads(response.read(65536))
        beans = value.get("beans", [])
        if len(beans) != 1 or beans[0].get("NumLiveDataNodes") != 2 or beans[0].get("NumDeadDataNodes") != 0:
            raise ValueError("Both existing HDFS data nodes must remain live")


def checked_guest_total(node, total):
    # MemTotal excludes guest/kernel reservations. The 1792 MiB allocation is
    # a separate compute-only candidate band, not a lower floor for control.
    bands = {"snow-control": ((1800, 2048),), "snow-compute": ((1664, 1792), (1800, 2048)),
             "snow-analysis": ((640, 768),)}
    if (node not in bands or type(total) is not int
            or not any(minimum * 1024 <= total <= maximum * 1024 for minimum, maximum in bands[node])):
        raise ValueError("Use the explicitly selected small offline VM sizes; this tool never reconfigures a VM")


def checked_snapshot(node, docker, original=None):
    value = docker.snapshot(node)
    total, available = docker.memory()
    checked_guest_total(node, total)
    if available < 128 * 1024:
        raise ValueError("Keep at least 128 MiB actual available guest memory during this bounded phase")
    if original:
        for service, entry in value.items():
            old = original[service]
            if entry["id"] != old["id"] or entry["identity_sha256"] != old["identity_sha256"]:
                raise ValueError("Stage container image, config, mounts, limits or identity changed")
            if service in {"namenode", "datanode"} and entry != old:
                raise ValueError("The untouched HDFS container changed during the lake phase")
    return value, available


def _record(root, config, run_id, attempt, node):
    if node not in SERVICES:
        raise ValueError("Unknown fixed stage node")
    return location(root, config, run_id, attempt) / ("stage-" + node + ".json")


def _lease(root, config, run_id, attempt, node):
    path = Path(root).absolute() / "runtime/real/lake-stage/owner.json"
    if path.resolve() != path:
        raise ValueError("Stage lease cannot traverse a link")
    value = dict(node=node, lane=config["lane"], run_id=run_id, attempt=attempt, config_sha256=digest(canonical(config)))
    if path.exists() and json.loads(read(path, 65536)) != value:
        raise ValueError("Another lake attempt owns this node's phase; explicit recovery is required")
    return path, value


def reserve_stage(root, config, run_id, attempt, node, *, docker=None):
    docker = docker or StageDocker(root, config)
    target = _record(root, config, run_id, attempt, node)
    lease, ownership = _lease(root, config, run_id, attempt, node)
    with publication_lock(lease.parent):
        _lease(root, config, run_id, attempt, node)
        original = json.loads(read(target, 65536)) if target.exists() else None
        value, available = checked_snapshot(node, docker, original["containers"] if original else None)
        if {key for key, entry in value.items() if entry["running"]} != BASELINE[node]:
            raise ValueError("Enter with NN+Hive and both DNs, with existing RM/NM stopped")
        if node == "snow-control" and available < 768 * 1024:
            raise ValueError("Control needs at least 768 MiB actual available memory for the catalog worker")
        if node == "snow-control":
            docker.hdfs_ready()
        if original:
            if original["config_sha256"] != digest(canonical(config)):
                raise ValueError("Stage reservation belongs to another exact config")
            write_json(lease, ownership)
            return original
        result = dict(schema_version=1, node=node, config_sha256=digest(canonical(config)),
                      containers=value, reserved_at=datetime.now(UTC).isoformat())
        write_json(target, result)
        write_json(lease, ownership)
        return result


def switch_stage(root, config, run_id, attempt, node, *, restore=False, docker=None):
    docker = docker or StageDocker(root, config)
    target = _record(root, config, run_id, attempt, node)
    lease, ownership = _lease(root, config, run_id, attempt, node)
    with publication_lock(lease.parent):
        _lease(root, config, run_id, attempt, node)
        if not target.exists():
            if restore and not lease.exists():
                return dict(node=node, stage_unreserved=True, no_container_mutation=True)
            raise ValueError("Stage owner exists without its exact reservation; recovery needs review")
        original = json.loads(read(target, 65536))
        if original["node"] != node or original["config_sha256"] != digest(canonical(config)):
            raise ValueError("Stage reservation belongs to another node/config")
        if not lease.exists():
            if not restore:
                raise ValueError("This attempt has no live stage reservation")
            # A completed attempt cannot later stop an ordinary new YARN job
            # just because the same container IDs were reused by its operator.
            value, available = checked_snapshot(node, docker, original["containers"])
            if ({key for key, entry in value.items() if entry["running"]} != BASELINE[node]
                    or node == "snow-control" and available < 768 * 1024):
                raise ValueError("Released stage ownership cannot mutate a later running phase")
            if node == "snow-control":
                docker.hdfs_ready()
            return dict(node=node, phase="hive", hdfs_unchanged=True, already_restored=True)
        if json.loads(read(lease, 65536)) != ownership:
            raise ValueError("This attempt has no live stage reservation")
        if restore:
            # Stop only our originally stopped compute IDs first. An HDFS or
            # unrelated-service change must not prevent this safe stop; such a
            # change still blocks starting Hive and claiming full recovery.
            for service in ("resourcemanager", "nodemanager"):
                if service in original["containers"]:
                    docker.stop_owned(original["containers"][service])
        value, _ = checked_snapshot(node, docker, original["containers"])
        desired = BASELINE[node] if restore else YARN[node]
        # Stop unwanted compute services before starting the other phase.
        order = ("nodemanager",) if node == "snow-compute" else ("resourcemanager", "hive")
        for service in order:
            if service in value and value[service]["running"] and service not in desired:
                docker.change(value[service]["id"], "stop")
                value, _ = checked_snapshot(node, docker, original["containers"])
                if value[service]["running"]:
                    raise ValueError("Owned compute service did not stop; refusing the next phase")
        for service in sorted(desired - {"namenode", "datanode"}):
            if not value[service]["running"]:
                docker.change(value[service]["id"], "start")
            deadline = time.monotonic() + 60
            while True:
                value, _ = checked_snapshot(node, docker, original["containers"])
                if not value[service]["running"]:
                    raise ValueError("The existing phase service stopped during readiness")
                try:
                    if docker.ready(node, service):
                        break
                except (OSError, ValueError):
                    pass
                if time.monotonic() >= deadline:
                    raise TimeoutError("Fixed lab service did not become ready within 60 seconds")
                time.sleep(1)
        value, available = checked_snapshot(node, docker, original["containers"])
        if {key for key, entry in value.items() if entry["running"]} != desired:
            raise ValueError("Exact phase running set differs")
        if restore and node == "snow-control" and available < 768 * 1024:
            raise ValueError("Restored Hive stage lacks the required 768 MiB catalog headroom")
        if node == "snow-control":
            docker.hdfs_ready()
        result = dict(schema_version=1, node=node, phase="hive" if restore else "yarn",
                      original_sha256=digest(canonical(original)), checked_at=datetime.now(UTC).isoformat(),
                      hdfs_unchanged=True, available_kib=available, containers=value)
        write_json(target.with_name("stage-result-" + node + ".json"), result)
        if restore:
            lease.unlink(missing_ok=True)
        return result
