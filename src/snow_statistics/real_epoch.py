"""A finite physical storage epoch for independently owned real lab engines.

Deletion means verified removal of Docker storage objects, not forensic erasure
of historical VMDK/SSD blocks. No shared prune, host directory delete, or online
collector operation is exposed. Fresh imports cannot renew an epoch's deadline.
"""
import base64
import ipaddress
import json
import os
import re
import signal
import socket
import subprocess
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

from .io import digest, write_json
from .lifecycle import timestamp
from .publication import PublicationLockBusy, canonical, publication_lock
from .real_sync_retention import cleanup_epoch_sync

OWNER = "Snow_Statistics"
VOLUMES = ("kafka", "doris-fe", "doris-be", "checkpoints", "flink-state")
ENGINES = ("kafka", "doris-fe", "doris-be", "jobmanager", "taskmanager")
LABEL_PREFIX = "org.snow-statistics."
ROOT = Path(__file__).resolve().parents[2]
KAFKA_TMPFS = {"/etc/kafka/secrets": "rw,noexec,nosuid,size=4m,uid=1000,gid=1000,mode=0750",
               "/mnt/shared/config": "rw,noexec,nosuid,size=16m,uid=1000,gid=1000,mode=0750"}


def labels(manifest):
    return {LABEL_PREFIX + key: str(manifest[value]) for key, value in (
        ("owner", "owner"), ("source", "source"), ("epoch", "epoch_id"),
        ("generation", "generation"), ("expires-at", "expires_at"), ("input-origin", "input_origin"))}


def verify_manifest(manifest):
    epoch = manifest["epoch_id"]
    if (manifest["schema_version"] != 1 or manifest["owner"] != OWNER or manifest["source"] != "real"
            or not re.fullmatch(r"[a-z][a-z0-9-]{2,23}", epoch)
            or manifest["mode"] not in {"engines", "fixture", "synthetic_engine_test"}):
        raise ValueError("Invalid owned real engine epoch")
    if manifest["mode"] in {"fixture", "synthetic_engine_test"} and (not epoch.startswith("fixture-") or manifest["input_origin"] != "synthetic fixtures"):
        raise ValueError("Physical cleanup fixture cannot be presented as real engine data")
    if manifest["mode"] == "engines" and manifest["input_origin"] != "real":
        raise ValueError("Real engines need explicitly real input provenance")
    if manifest["event_lane"] != epoch.replace("-", "_"):
        raise ValueError("Epoch event lane must use its immutable normalized identity")
    if timestamp(manifest["expires_at"]) != timestamp(manifest["original_min_accepted_at"]) + timedelta(days=7):
        raise ValueError("Epoch deadline must remain seven days from original acceptance")
    project = "snow-real-" + epoch
    expected = {role: project + "-" + role for role in (ENGINES if manifest["mode"] != "fixture" else ("fixture",))}
    if (manifest["project"] != project or manifest["containers"] != expected or
            manifest["volumes"] != {role: project + "-" + role for role in VOLUMES}):
        raise ValueError("Epoch resource names escaped the exact owned scope")
    if manifest["host"] != "snow-analysis":
        raise ValueError("Real engine epochs belong only on the isolated analysis VM")
    UUID(manifest["generation"])
    return manifest


def make_manifest(epoch, original_at, images, *, fixture=False, synthetic_engine_test=False, now=None):
    current = now or datetime.now(UTC)
    origin = timestamp(original_at)
    if fixture and synthetic_engine_test:
        raise ValueError("Choose physical fixture or synthetic engine test, not both")
    if origin > current or origin + timedelta(days=7) <= current:
        raise ValueError("New epoch needs an unexpired original acceptance window")
    project = "snow-real-" + epoch
    required = {"PYTHON_IMAGE"} if fixture else {"KAFKA_IMAGE", "DORIS_FE_IMAGE", "DORIS_BE_IMAGE", "FLINK_IMAGE"}
    if set(images) != required or any(not re.fullmatch(r"[A-Za-z0-9./:_-]+@sha256:[a-f0-9]{64}", v) for v in images.values()):
        raise ValueError("Epoch images must use the existing explicit digest locks")
    value = dict(schema_version=1, owner=OWNER, source="real", epoch_id=epoch, generation=str(uuid4()),
                 mode="fixture" if fixture else "synthetic_engine_test" if synthetic_engine_test else "engines",
                 input_origin="synthetic fixtures" if fixture or synthetic_engine_test else "real",
                 original_min_accepted_at=origin.isoformat(), expires_at=(origin + timedelta(days=7)).isoformat(),
                 project=project, event_lane=epoch.replace("-", "_"), host="snow-analysis", images=images,
                 containers={role: project + "-" + role for role in (("fixture",) if fixture else ENGINES)},
                 volumes={role: project + "-" + role for role in VOLUMES})
    return verify_manifest(value)


def compose_spec(manifest, directory, analysis_ip=None, jar=None):
    verify_manifest(manifest)
    label = labels(manifest)
    bounds = dict(restart="no", labels=label, pids_limit=256,
                  logging=dict(driver="local", options={"max-size": "2m", "max-file": "2"}))
    common_env = {"SNOW_EPOCH_EXPIRES_UNIX": str(int(timestamp(manifest["expires_at"]).timestamp()))}
    guard = str(Path(directory).absolute() / "epoch-guard.sh") + ":/snow/epoch-guard.sh:ro"

    def service(role, image, command, mounts, memory, cpus, environment=None):
        return bounds | dict(container_name=manifest["containers"][role], image=image, entrypoint=["sh", "/snow/epoch-guard.sh"],
                             pids_limit=512 if role == "doris-be" else 256,
                             command=command, volumes=[guard, *mounts], mem_limit=memory, cpus=cpus,
                             environment=common_env | (environment or {}))

    images = manifest["images"]
    if manifest["mode"] == "fixture":
        # Only unidentifiable synthetic marker bytes. This exercises real Docker
        # deletion APIs without claiming Kafka/Doris/Flink engine integration.
        services = {"fixture": service("fixture", images["PYTHON_IMAGE"], ["python", "-c",
                     "from pathlib import Path; import time; "
                     "[Path('/fixture/'+x+'/marker').write_text('synthetic cleanup fixture') "
                     "for x in ('kafka','doris-fe','doris-be','checkpoints','flink-state')]; time.sleep(3600)"],
                    [name + ":/fixture/" + name for name in VOLUMES], "64m", 0.1)}
        services["fixture"]["network_mode"] = "none"
    else:
        if not ipaddress.ip_address(analysis_ip).is_private:
            raise ValueError("Use the explicitly configured private analysis VM IP")
        target = Path(jar).absolute()
        if target.resolve() != target or target.suffix != ".jar" or not target.is_file():
            raise ValueError("Expected a real, immutable Flink JAR file")
        fe = ["bash", "-ec", "cat /snow/fe.conf >> /opt/apache-doris/fe/conf/fe.conf; "
              "printf '\\npriority_networks = %s/32\\n' \"$$SNOW_DORIS_IP\" >> /opt/apache-doris/fe/conf/fe.conf; exec bash init_fe.sh"]
        be = ["bash", "-ec", "sh /snow/prepare-be-start.sh; cat /snow/be.conf >> /opt/apache-doris/be/conf/be.conf; "
              "printf '\\npriority_networks = %s/32\\n' \"$$SNOW_DORIS_IP\" >> /opt/apache-doris/be/conf/be.conf; exec bash entry_point.sh"]
        flink_env = dict(KAFKA_BOOTSTRAP=analysis_ip + ":9092", SNOW_SOURCE="real", SNOW_REPLAY_LANE=manifest["event_lane"],
                         SNOW_INPUT_TOPIC="snow.real." + manifest["event_lane"] + ".events.v1",
                         SNOW_REAL_EPOCH_ID=manifest["epoch_id"], SNOW_REAL_EPOCH_GENERATION=manifest["generation"],
                         SNOW_REAL_EPOCH_FROM=manifest["original_min_accepted_at"], SNOW_REAL_EPOCH_UNTIL=manifest["expires_at"],
                         DORIS_FE=analysis_ip + ":8030", DORIS_TABLE="snow_real_" + manifest["epoch_id"].replace("-", "_") + ".events_realtime",
                         FLINK_PROPERTIES="\n".join(("jobmanager.rpc.address: 127.0.0.1", "jobmanager.bind-host: 127.0.0.1",
                           "rest.address: 127.0.0.1", "rest.bind-address: 127.0.0.1", "jobmanager.memory.process.size: 768m",
                           "taskmanager.host: 127.0.0.1", "taskmanager.bind-host: 127.0.0.1", "taskmanager.numberOfTaskSlots: 1",
                           "taskmanager.memory.process.size: 1280m", "taskmanager.memory.managed.size: 0m",
                           "state.checkpoints.dir: file:///checkpoints", "state.savepoints.dir: file:///checkpoints/savepoints",
                           "state.checkpoints.num-retained: 3", "io.tmp.dirs: /flink-state/tmp", "parallelism.default: 1")))
        flink_mounts = ["checkpoints:/checkpoints", "flink-state:/flink-state", str(target) + ":/opt/flink/usrlib/snow-realtime.jar:ro"]
        services = {
            "kafka": service("kafka", images["KAFKA_IMAGE"], ["/etc/kafka/docker/run"], ["kafka:/var/lib/kafka/data"], "640m", 1,
                             dict(KAFKA_NODE_ID="1", KAFKA_PROCESS_ROLES="broker,controller",
                                  KAFKA_LISTENERS="PLAINTEXT://:9092,CONTROLLER://127.0.0.1:9093",
                                  KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://" + analysis_ip + ":9092",
                                  KAFKA_CONTROLLER_LISTENER_NAMES="CONTROLLER",
                                  KAFKA_LISTENER_SECURITY_PROTOCOL_MAP="CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
                                  KAFKA_CONTROLLER_QUORUM_VOTERS="1@127.0.0.1:9093", KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR="1",
                                  KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR="1", KAFKA_TRANSACTION_STATE_LOG_MIN_ISR="1",
                                  KAFKA_AUTO_CREATE_TOPICS_ENABLE="false", KAFKA_LOG_DIRS="/var/lib/kafka/data",
                                  KAFKA_LOG_RETENTION_MS="604800000", KAFKA_LOG_ROLL_MS="60000",
                                  KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS="60000", KAFKA_LOG_SEGMENT_DELETE_DELAY_MS="1000",
                                  KAFKA_LOG_RETENTION_BYTES="268435456", KAFKA_HEAP_OPTS="-Xms128m -Xmx384m",
                                  CLUSTER_ID=base64.urlsafe_b64encode(UUID(manifest["generation"]).bytes).decode().rstrip("="))),
            "doris-fe": service("doris-fe", images["DORIS_FE_IMAGE"], fe,
                                ["doris-fe:/opt/apache-doris/fe/doris-meta", str(Path(directory).absolute() / "fe.conf") + ":/snow/fe.conf:ro"],
                                "1280m", 1, dict(FE_SERVERS="fe1:" + analysis_ip + ":9010", FE_ID="1", SNOW_DORIS_IP=analysis_ip)),
            "doris-be": service("doris-be", images["DORIS_BE_IMAGE"], be,
                                ["doris-be:/opt/apache-doris/be/storage", str(Path(directory).absolute() / "be.conf") + ":/snow/be.conf:ro",
                                 str(Path(directory).absolute() / "prepare-be-start.sh") + ":/snow/prepare-be-start.sh:ro"],
                                "1792m", 2, dict(FE_SERVERS="fe1:" + analysis_ip + ":9010", BE_ADDR=analysis_ip + ":9050", SNOW_DORIS_IP=analysis_ip)),
            "jobmanager": service("jobmanager", images["FLINK_IMAGE"], ["bash", "-ec",
                                  "mkdir -p /flink-state/tmp; chown flink:flink /checkpoints /flink-state /flink-state/tmp; exec /docker-entrypoint.sh jobmanager"],
                                  flink_mounts, "896m", 1, flink_env),
            "taskmanager": service("taskmanager", images["FLINK_IMAGE"], ["bash", "-ec",
                                   "mkdir -p /flink-state/tmp; chown flink:flink /checkpoints /flink-state /flink-state/tmp; exec /docker-entrypoint.sh taskmanager"],
                                   flink_mounts, "1536m", 2, flink_env),
        }
        for value in services.values():
            value["network_mode"] = "host"
        # The locked Kafka image declares both paths as VOLUME. Override them so
        # Docker cannot create unowned anonymous storage outside the epoch.
        services["kafka"]["tmpfs"] = [path + ":" + options for path, options in KAFKA_TMPFS.items()]
        for role in ("jobmanager", "taskmanager"):
            services[role]["user"] = "0:0"  # Initialize only this epoch's volumes; Flink entrypoint drops UID.
    return dict(name=manifest["project"], services=services,
                volumes={role: dict(name=name, labels=label) for role, name in manifest["volumes"].items()})


class DockerEpoch:
    """Minimal Docker API wrapper; stdout returned only as checked metadata."""
    def __init__(self, host="snow-analysis"):
        if socket.gethostname() != host or host != "snow-analysis":
            raise ValueError("Physical epoch operations are restricted to snow-analysis")

    def command(self, arguments, *, timeout=30):
        result = subprocess.run(["sudo", "docker", *arguments], capture_output=True, timeout=timeout)
        if result.returncode:
            raise RuntimeError("Scoped Docker operation failed: " + arguments[0])
        if len(result.stdout) > 2097152:
            raise ValueError("Docker metadata response exceeded bound")
        return result.stdout

    def containers(self):
        value = self.command(["ps", "-a", "--format", "{{.Names}}"])
        return set(value.decode().splitlines())

    def volumes(self):
        return set(self.command(["volume", "ls", "--format", "{{.Name}}"] ).decode().splitlines())

    def inspect_container(self, name):
        value = json.loads(self.command(["inspect", name]))[0]
        deadline = next((item.split("=", 1)[1] for item in value["Config"]["Env"]
                         if item.startswith("SNOW_EPOCH_EXPIRES_UNIX=")), None)
        return dict(labels=value["Config"]["Labels"] or {}, running=value["State"]["Running"],
                    restart=value["HostConfig"]["RestartPolicy"]["Name"], mounts=value["Mounts"],
                    entrypoint=value["Config"]["Entrypoint"], deadline=deadline, tmpfs=value["HostConfig"].get("Tmpfs") or {})

    def inspect_volume(self, name):
        return json.loads(self.command(["volume", "inspect", name]))[0]

    def stop(self, name):
        self.command(["stop", "--time", "15", name], timeout=25)

    def remove_container(self, name):
        self.command(["rm", name])  # No force; a running/restarted object fails closed.

    def remove_volume(self, name):
        self.command(["volume", "rm", name])  # No force and no shared prune.

    def start(self, compose, services):
        self.command(["compose", "-f", str(compose), "up", "-d", *services], timeout=180)

    def admit(self, manifest, stage, starting):
        import shutil
        minimum_disk = (128 if manifest["mode"] == "fixture" else 1536 if stage == "storage" else 256) * 1024**2
        memory = dict(line.split(":", 1) for line in Path("/proc/meminfo").read_text().splitlines())
        available = int(memory["MemAvailable"].split()[0]) * 1024
        minimum_ram = (128 if manifest["mode"] == "fixture" else (3584 if stage == "storage" else 1536) if starting else 256) * 1024**2
        free = shutil.disk_usage("/").free
        if free < minimum_disk or available < minimum_ram:
            raise ValueError("Insufficient analysis VM disk or memory for this isolated epoch")
        return dict(available_ram_bytes=available, available_disk_bytes=free, stage=stage,
                    minimum_ram_bytes=minimum_ram, minimum_disk_bytes=minimum_disk,
                    measured_engine_capacity=False)


class Epoch:
    def __init__(self, directory, docker):
        self.directory = Path(directory).absolute()
        if self.directory.resolve() != self.directory:
            raise ValueError("Epoch metadata cannot traverse links")
        self.docker = docker

    def read(self):
        value = verify_manifest(json.loads((self.directory / "manifest.json").read_bytes()))
        if self.directory.name != value["epoch_id"]:
            raise ValueError("Epoch directory and owner identity differ")
        return value

    def _inspect(self, manifest):
        expected = labels(manifest)
        containers, volumes = {}, {}
        existing_c, existing_v = self.docker.containers(), self.docker.volumes()
        for name in manifest["containers"].values():
            if name not in existing_c:
                continue
            data = self.docker.inspect_container(name)
            if any(data["labels"].get(k) != v for k, v in expected.items()) or data["restart"] not in ("", "no"):
                raise ValueError("Container ownership or restart policy differs from its epoch")
            if (data["entrypoint"] != ["sh", "/snow/epoch-guard.sh"] or
                    data["deadline"] != str(int(timestamp(manifest["expires_at"]).timestamp()))):
                raise ValueError("Container lost its immutable expiry guard")
            expected_tmpfs = KAFKA_TMPFS if name == manifest["containers"].get("kafka") else {}
            if data.get("tmpfs", {}) != expected_tmpfs:
                raise ValueError("Container temporary storage differs from the bounded epoch scope")
            for mount in data["mounts"]:
                if mount["Type"] == "volume" and mount["Name"] in manifest["volumes"].values():
                    continue
                if mount["Type"] == "bind" and not mount["RW"]:
                    continue
                if mount["Type"] == "tmpfs" and mount["Destination"] in expected_tmpfs:
                    continue
                raise ValueError("Unknown writable or anonymous storage in real epoch")
            containers[name] = data
        for name in manifest["volumes"].values():
            if name in existing_v:
                data = self.docker.inspect_volume(name)
                if data["Driver"] != "local" or data.get("Options") or any((data.get("Labels") or {}).get(k) != v for k, v in expected.items()):
                    raise ValueError("Volume ownership or external driver differs from its epoch")
                volumes[name] = data
        return containers, volumes

    def stop(self):
        manifest = self.read()
        # Stopping readers is safe even when storage auditing failed. Never let
        # an unexpected writable mount keep this exact owned process running.
        # Retirement still performs the complete _inspect before any mutation.
        expected, containers = labels(manifest), {}
        for name in set(manifest["containers"].values()) & self.docker.containers():
            data = self.docker.inspect_container(name)
            if any(data["labels"].get(k) != v for k, v in expected.items()):
                raise ValueError("Cannot stop a container whose epoch ownership changed")
            containers[name] = data
        for name, value in containers.items():
            if value["running"]:
                self.docker.stop(name)
        if any(self.docker.inspect_container(name)["running"] for name in containers):
            raise RuntimeError("Real epoch still has running readers")
        return dict(epoch_id=manifest["epoch_id"], stopped=list(containers), data_deleted=False)

    def retire(self, *, now=None, fixture_clock=False):
        manifest = self.read()
        current = now or datetime.now(UTC)
        if fixture_clock and manifest["mode"] not in {"fixture", "synthetic_engine_test"}:
            raise ValueError("Only synthetic physical fixtures may advance a test clock")
        if now is not None and not fixture_clock:
            raise ValueError("Engine expiry uses the real wall clock")
        if timestamp(manifest["expires_at"]) > current:
            raise ValueError("An unexpired engine epoch cannot be retired")
        with publication_lock(self.directory):
            previous = self.directory / "retirement.json"
            if previous.exists():
                value = json.loads(previous.read_bytes())
                if (value["owner_manifest_sha256"] != digest(canonical(manifest))
                        or set(manifest["containers"].values()) & self.docker.containers()
                        or set(manifest["volumes"].values()) & self.docker.volumes()):
                    raise ValueError("Retired epoch was changed or its resources reappeared")
                return value
            write_json(self.directory / "gate.json", dict(open=False, reason="physical_retirement_in_progress"))
            # Validate every container and volume before making the first mutation.
            containers, volumes = self._inspect(manifest)
            self.stop()
            for name in containers:
                self.docker.remove_container(name)
            if set(manifest["containers"].values()) & self.docker.containers():
                raise RuntimeError("Expired container remains after removal")
            for name in volumes:
                self.docker.remove_volume(name)
            if set(manifest["volumes"].values()) & self.docker.volumes():
                raise RuntimeError("Expired volume remains after removal")
            evidence = dict(source="real", input_origin=manifest["input_origin"], epoch_id=manifest["epoch_id"],
                            generation=manifest["generation"], owner_manifest_sha256=digest(canonical(manifest)),
                            original_min_accepted_at=manifest["original_min_accepted_at"], expires_at=manifest["expires_at"],
                            containers_stopped=sorted(containers), containers_removed=sorted(containers),
                            volumes_removed=sorted(volumes),
                            readback_absent={"containers": sorted(manifest["containers"].values()),
                                             "volumes": sorted(manifest["volumes"].values())},
                            checked_at=current.isoformat(), physical_storage_scope="Docker objects and writable layers",
                            forensic_media_erasure_claimed=False, production_online_database_touched=False)
            result = evidence | {"evidence_sha256": digest(canonical(evidence))}
            write_json(self.directory / "retirement.json", result)
            return result

    def start(self, stage="storage"):
        manifest = self.read()
        if stage not in {"storage", "realtime"}:
            raise ValueError("Start storage and realtime in explicit stages")
        if (timestamp(manifest["expires_at"]) <= datetime.now(UTC) or (self.directory / "retirement.json").exists()
                or (self.directory / "failed-fixture-retirement.json").exists()):
            raise ValueError("Expired or retired epoch must never restart")
        if (self.directory / "gate.json").exists():
            gate = json.loads((self.directory / "gate.json").read_bytes())
            if gate.get("reason") == "physical_retirement_in_progress":
                raise ValueError("Finish failed physical retirement before any restart")
        # No different lab/real containers may use this VM's host ports or memory.
        for name in self.docker.containers() - set(manifest["containers"].values()):
            if self.docker.inspect_container(name)["running"]:
                raise ValueError("Stop other analysis profiles before starting this epoch")
        prior, _ = self._inspect(manifest)
        roles = (["fixture"] if manifest["mode"] == "fixture" else
                 ["kafka", "doris-fe", "doris-be"] if stage == "storage" else ["jobmanager", "taskmanager"])
        if manifest["mode"] != "fixture" and stage == "realtime":
            if any(manifest["containers"][role] not in prior or not prior[manifest["containers"][role]]["running"]
                   for role in ("kafka", "doris-fe", "doris-be")):
                raise ValueError("Start and validate the storage stage before realtime")
        starting = any(manifest["containers"][role] not in prior or not prior[manifest["containers"][role]]["running"] for role in roles)
        capacity = self.docker.admit(manifest, stage, starting)
        write_json(self.directory / "capacity-preflight.json", capacity)
        for filename, expected in manifest["files"].items():
            path = self.directory / filename
            if path.resolve() != path.absolute() or digest(path.read_bytes()) != expected:
                raise ValueError("Epoch configuration changed after its owner manifest")
        if "jar" in manifest and digest(Path(manifest["jar"]["path"]).read_bytes()) != manifest["jar"]["sha256"]:
            raise ValueError("The frozen epoch JAR changed")
        try:
            self.docker.start(self.directory / "compose.json", roles)
            containers, volumes = self._inspect(manifest)
            required_c = {manifest["containers"][role] for role in roles}
            required_v = ({manifest["volumes"][role] for role in roles} if manifest["mode"] != "fixture" and stage == "storage"
                          else set(manifest["volumes"].values()))
            if (not required_c <= set(containers) or not required_v <= set(volumes)
                    or any(not containers[name]["running"] for name in required_c)):
                raise RuntimeError("Epoch creation is incomplete; no read permit")
        except Exception:
            self.stop()
            raise
        complete = manifest["mode"] == "fixture" or stage == "realtime"
        write_json(self.directory / "gate.json", dict(open=complete and manifest["mode"] == "engines",
                                                       serves_real_data=manifest["mode"] == "engines",
                                                       fixture_ready=complete and manifest["mode"] != "engines",
                                                       reason="started" if complete else "storage_stage_only",
                                                       expires_at=manifest["expires_at"], owner_manifest_sha256=digest(canonical(manifest))))
        return dict(epoch_id=manifest["epoch_id"], started=True, deadline=manifest["expires_at"], source="real",
                    input_origin=manifest["input_origin"], stage=stage, engines_verified=False)

    def readable(self):
        """Physical epoch gate; callers must still run logical backend permits."""
        manifest = self.read()
        gate = json.loads((self.directory / "gate.json").read_bytes())
        owner_hash = digest(canonical(manifest))
        if (manifest["mode"] != "engines" or manifest["input_origin"] != "real" or
                not gate.get("open") or not gate.get("serves_real_data") or
                gate.get("owner_manifest_sha256") != owner_hash or
                timestamp(manifest["expires_at"]) <= datetime.now(UTC) or (self.directory / "retirement.json").exists()):
            raise ValueError("Physical real engine epoch is not readable")
        containers, volumes = self._inspect(manifest)
        if (set(containers) != set(manifest["containers"].values()) or
                set(volumes) != set(manifest["volumes"].values()) or any(not value["running"] for value in containers.values())):
            raise ValueError("Real engine readers or storage are missing")
        return dict(source="real", epoch_id=manifest["epoch_id"], generation=manifest["generation"],
                    owner_manifest_sha256=owner_hash, expires_at=manifest["expires_at"],
                    original_min_accepted_at=manifest["original_min_accepted_at"], event_lane=manifest["event_lane"])


def prepare(directory, manifest, analysis_ip=None, jar=None):
    directory = Path(directory).absolute()
    if directory.name != manifest["epoch_id"] or directory.exists():
        raise ValueError("Create one new empty epoch directory only")
    if directory.resolve() != directory:
        raise ValueError("Epoch preparation path traverses a link")
    directory.mkdir(parents=True, mode=0o700)
    templates = ROOT / "deploy/real-epoch"
    for name in ("epoch-guard.sh", "fe.conf", "be.conf", "prepare-be-start.sh"):
        (directory / name).write_bytes((templates / name).read_bytes().replace(b"\r\n", b"\n"))
    write_json(directory / "compose.json", compose_spec(manifest, directory, analysis_ip, jar))
    manifest["files"] = {name: digest((directory / name).read_bytes()) for name in ("compose.json", "epoch-guard.sh", "fe.conf", "be.conf", "prepare-be-start.sh")}
    if jar:
        manifest["jar"] = dict(path=str(Path(jar).absolute()), sha256=digest(Path(jar).read_bytes()))
    write_json(directory / "manifest.json", manifest)
    write_json(directory / "gate.json", dict(open=False, reason="prepared_not_started"))
    return manifest


def expire_due(root, docker):
    root = Path(root).absolute()
    if root.resolve() != root:
        raise ValueError("Epoch registry contains a link")
    results = []
    for directory in sorted(root.iterdir()):
        if not directory.is_dir():
            raise ValueError("Unexpected file in the dedicated epoch registry")
        epoch = Epoch(directory, docker)
        manifest = epoch.read()
        cleanup_epoch_sync(directory, manifest)
        if timestamp(manifest["expires_at"]) <= datetime.now(UTC):
            results.append(epoch.retire())
    return results


def supervise(root, docker):
    """All starts happen separately after this supervisor's cleanup succeeds.

    Container PID1 guards also stop readers at expiry; this process removes their
    volumes and writable layers. Concurrent cleanup may hold the retirement lock;
    retry that acquisition for at most 60 seconds, never other I/O failures.
    Shutdown preserves data but stops owned readers.
    """
    def stopping(*_):
        raise SystemExit(0)
    signal.signal(signal.SIGTERM, stopping)
    signal.signal(signal.SIGINT, stopping)
    try:
        while True:
            _expire_due_with_lock_retry(root, docker)
            expiries = [timestamp(Epoch(path, docker).read()["expires_at"]).timestamp()
                        for path in Path(root).iterdir() if not (path / "retirement.json").exists()]
            delay = min(30, max(0.1, min(expiries) - time.time())) if expiries else 30
            time.sleep(delay)
    finally:
        stop_all(root, docker)


def _expire_due_with_lock_retry(root, docker):
    """Bound each cleanup cycle; successful cleanup gives the next cycle a new budget."""
    deadline = time.monotonic() + 60
    while True:
        try:
            return expire_due(root, docker)
        except PublicationLockBusy:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise
            time.sleep(min(1, remaining))


def stop_all(root, docker):
    results = []
    for directory in Path(root).iterdir():
        results.append(Epoch(directory, docker).stop())
    return results


def private_epoch_root(root):
    path = Path(root).absolute()
    if path.resolve() != path or not path.as_posix().endswith("/runtime/real/epochs"):
        raise ValueError("Use a dedicated runtime/real/epochs registry")
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    if os.name == "posix" and path.stat().st_mode & 0o077:
        raise ValueError("Epoch runtime registry must be private")
    return path
