"""Admission for stopped, still-live physical epochs; no SQL cleanup is implied.

Only in-process adapters which perform actual initialization/job readbacks may
create writer registrations. There is deliberately no CLI accepting success
JSON. The production initializer/submitter must supply these adapters.
"""
import base64
import json
import re
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from uuid import UUID

from .io import digest, write_json
from .landing import collector_identity
from .lifecycle import RETENTION_DAYS, timestamp
from .publication import canonical, publication_lock
from .real_epoch import LABEL_PREFIX, DockerEpoch, expire_due

ROOT = Path(__file__).resolve().parents[2]
TABLES = ("events_realtime", "daily_offline", "daily_snapshots", "offline_releases")
TOPICS = ("events", "quarantine", "duplicates", "late")
STATES = ("/checkpoints", "/flink-state")
WRITER_FILES = ("src/snow_statistics/sync.py", "src/snow_statistics/real_quiescent.py",
                "src/snow_statistics/real_lab.py", "src/snow_statistics/publication.py",
                "src/snow_statistics/real_writer.py", "tools/real_writer.py",
                "src/snow_statistics/real_transfer.py",
                "src/snow_statistics/real_writer_recovery.py", "tools/real_writer_recovery.py",
                "warehouse/doris/schema.sql", "warehouse/doris/publication.sql")


def bounded_json(path):
    path = Path(path).absolute()
    if path.resolve() != path or path.stat().st_size > 262144:
        raise ValueError("Writer metadata must be bounded and cannot traverse links")
    return json.loads(path.read_bytes())


def same_keys(value, keys, name):
    if not isinstance(value, dict) or set(value) != set(keys):
        raise ValueError("Unexpected " + name + " fields")


def frozen(epoch):
    manifest = epoch.read()
    if manifest["mode"] != "engines" or manifest["input_origin"] != "real":
        raise ValueError("Synthetic engine receipts cannot authorize real writers")
    if set(manifest.get("files", {})) != {"compose.json", "epoch-guard.sh", "fe.conf", "be.conf", "prepare-be-start.sh"}:
        raise ValueError("A real epoch needs every frozen configuration file")
    for name, expected in manifest["files"].items():
        path = epoch.directory / name
        if path.resolve() != path.absolute() or digest(path.read_bytes()) != expected:
            raise ValueError("Frozen epoch configuration changed")
    jar = manifest.get("jar", {})
    same_keys(jar, ("path", "sha256"), "frozen JAR")
    path = Path(jar["path"]).absolute()
    if path.resolve() != path or digest(path.read_bytes()) != jar["sha256"]:
        raise ValueError("Frozen epoch JAR changed")
    return manifest


class DockerStorage(DockerEpoch):
    """Actual Docker identity/creation metadata, retaining no private env values."""
    def inspect_container(self, name):
        value = json.loads(self.command(["inspect", name]))[0]
        env = value["Config"].get("Env") or []
        epoch_keys = {"SNOW_REAL_EPOCH_ID", "SNOW_REAL_EPOCH_GENERATION", "SNOW_REAL_EPOCH_FROM", "SNOW_REAL_EPOCH_UNTIL"}
        return dict(labels=value["Config"]["Labels"] or {}, running=value["State"]["Running"],
                    restart=value["HostConfig"]["RestartPolicy"]["Name"], mounts=value["Mounts"],
                    entrypoint=value["Config"]["Entrypoint"],
                    deadline=next((item.split("=", 1)[1] for item in env if item.startswith("SNOW_EPOCH_EXPIRES_UNIX=")), None),
                    object_id=value["Id"], created_at=value["Created"],
                    tmpfs=value["HostConfig"].get("Tmpfs") or {},
                    epoch_environment={item.split("=", 1)[0]: item.split("=", 1)[1] for item in env
                                       if item.split("=", 1)[0] in epoch_keys})


def storage(epoch, *, running):
    """Inspect all objects, rejecting hidden consumers and replacement copies."""
    manifest = frozen(epoch)
    containers, volumes = epoch._inspect(manifest)
    if (set(containers) != set(manifest["containers"].values()) or
            set(volumes) != set(manifest["volumes"].values())):
        raise ValueError("Every registered epoch container and volume must exist")
    if running is not None and any(value["running"] is not running for value in containers.values()):
        raise ValueError("Every epoch container must be " + ("running" if running else "stopped"))
    all_c, all_v = epoch.docker.containers(), epoch.docker.volumes()
    if len(all_c) > 256 or len(all_v) > 512:
        raise ValueError("Docker inventory exceeds the bounded admission scope")
    for name in all_c - set(containers):
        value = epoch.docker.inspect_container(name)
        if (value["labels"].get(LABEL_PREFIX + "epoch") == manifest["epoch_id"] or
                any(m.get("Name") in volumes for m in value["mounts"] if m["Type"] == "volume")):
            raise ValueError("An unregistered container shares this epoch or its volumes")
    for name in all_v - set(volumes):
        value = epoch.docker.inspect_volume(name)
        if (value.get("Labels") or {}).get(LABEL_PREFIX + "epoch") == manifest["epoch_id"]:
            raise ValueError("An unregistered volume shares this epoch")
    spec = bounded_json(epoch.directory / "compose.json")
    identities = {}
    for role, name in manifest["containers"].items():
        value = containers[name]
        if role in {"jobmanager", "taskmanager"} and value.get("epoch_environment") != {
                "SNOW_REAL_EPOCH_ID": manifest["epoch_id"], "SNOW_REAL_EPOCH_GENERATION": manifest["generation"],
                "SNOW_REAL_EPOCH_FROM": manifest["original_min_accepted_at"], "SNOW_REAL_EPOCH_UNTIL": manifest["expires_at"]}:
            raise ValueError("Actual Flink environment lost its immutable epoch binding")
        identifier = value.get("object_id", "")
        if not re.fullmatch(r"[a-f0-9]{64}", identifier):
            raise ValueError("Actual Docker container IDs are required")
        expected, binds = set(), set()
        for mount in spec["services"][role]["volumes"]:
            prefix = mount.split(":", 1)[0]
            if prefix in spec["volumes"]:
                expected.add(spec["volumes"][prefix]["name"])
            else:
                source, destination, mode = mount.rsplit(":", 2)
                if mode != "ro":
                    raise ValueError("Frozen configuration unexpectedly includes a writable bind")
                binds.add((str(Path(source).absolute()), destination))
        actual = {m["Name"] for m in value["mounts"] if m["Type"] == "volume"}
        actual_binds = {(m["Source"], m["Destination"]) for m in value["mounts"] if m["Type"] == "bind" and not m["RW"]}
        if actual != expected or binds != actual_binds:
            raise ValueError("Container volume mounts differ from the frozen composition")
        identities[name] = dict(object_id=identifier, created_at=value["created_at"])
    volume_identity = {}
    for name, value in volumes.items():
        # CreatedAt distinguishes recreation under the same name and labels.
        if not isinstance(value.get("CreatedAt"), str) or not value["CreatedAt"]:
            raise ValueError("Actual Docker volume creation metadata is required")
        volume_identity[name] = dict(created_at=value["CreatedAt"], labels=value["Labels"])
    return dict(containers=identities, volumes=volume_identity)


def topic_names(manifest):
    return ["snow.real." + manifest["event_lane"] + "." + name + ".v1" for name in TOPICS]


def writer_hashes():
    return {name: digest((ROOT / name).read_bytes()) for name in WRITER_FILES}


def expected_parameters(manifest, kafka):
    topic = topic_names(manifest)[0]
    return dict(epoch_id=manifest["epoch_id"], epoch_generation=manifest["generation"],
                epoch_from=manifest["original_min_accepted_at"], epoch_until=manifest["expires_at"],
                readable_from=manifest["original_min_accepted_at"], restore_not_after=manifest["expires_at"],
                source="real", lane=manifest["event_lane"], input_topic=topic,
                topic_id=kafka["identity"]["topic_ids"][topic], cluster_id=kafka["identity"]["cluster_id"],
                start_offset=kafka["bounds"][topic]["end"],
                doris_table="snow_real_" + manifest["event_lane"] + ".events_realtime")


def validate_initial(value, manifest):
    same_keys(value, ("kafka", "doris", "flink", "state"), "initial engine readback")
    kafka = value["kafka"]
    same_keys(kafka, ("identity", "bounds"), "Kafka readback")
    same_keys(kafka["identity"], ("cluster_id", "topic_ids"), "Kafka identity")
    names = topic_names(manifest)
    if set(kafka["identity"]["topic_ids"]) != set(names) or set(kafka["bounds"]) != set(names):
        raise ValueError("Read back every exact epoch input and side-output topic")
    ids = [kafka["identity"]["cluster_id"], *kafka["identity"]["topic_ids"].values()]
    if any(not isinstance(x, str) or not re.fullmatch(r"[A-Za-z0-9_-]{16,64}", x) for x in ids):
        raise ValueError("Actual Kafka cluster/topic IDs are required")
    expected_cluster = base64.urlsafe_b64encode(UUID(manifest["generation"]).bytes).decode().rstrip("=")
    if kafka["identity"]["cluster_id"] != expected_cluster or len(set(kafka["identity"]["topic_ids"].values())) != len(names):
        raise ValueError("Kafka cluster must match the frozen epoch generation and topics need distinct IDs")
    for entry in kafka["bounds"].values():
        same_keys(entry, ("partition", "start", "end"), "initial topic bounds")
        if any(type(v) is not int or v != 0 for v in entry.values()):
            raise ValueError("Register writers only after reading back fresh empty single-partition topics")
    database = "snow_real_" + manifest["event_lane"]
    same_keys(value["doris"], ("database", "tables"), "Doris initialization")
    if value["doris"]["database"] != database or set(value["doris"]["tables"]) != set(TABLES):
        raise ValueError("Read back the exact epoch database and all physical tables")
    if any(type(count) is not int or count != 0 for count in value["doris"]["tables"].values()):
        raise ValueError("Existing Doris payload cannot be adopted as a fresh epoch")
    if value["flink"] != {"jobs": []} or value["state"] != {name: [] for name in STATES}:
        raise ValueError("Fresh initialization requires actual empty jobs/checkpoint/state readbacks")
    return value


class WriterRegistry:
    """Immutable registration; probe is trusted executable code, never a file flag.

    probe.read_initial_state(manifest, collector) must read the four Kafka topic
    IDs/bounds, physical Doris tables, Flink jobs and both mounted state roots.
    probe.read_job(job_id) must read RUNNING job details and the parameters bound
    by its actual upload/submission, including the verified uploaded JAR hash.
    """
    def __init__(self, epoch):
        self.epoch = epoch
        self.path = epoch.directory / "writer-registration.json"
        self.job_path = epoch.directory / "writer-job.json"

    def initialize(self, collector, probe, *, now=None):
        current = now or datetime.now(UTC)
        manifest = frozen(self.epoch)
        identity = collector_identity(collector)
        with publication_lock(self.epoch.directory):
            if self.path.exists() or self.job_path.exists():
                raise ValueError("Writer registration is immutable; never reinitialize an existing epoch")
            if not timestamp(manifest["original_min_accepted_at"]) <= current < timestamp(manifest["expires_at"]):
                raise ValueError("Cannot initialize an expired or future epoch")
            before = storage(self.epoch, running=True)
            value = validate_initial(probe.read_initial_state(manifest, identity), manifest)
            if storage(self.epoch, running=True) != before:
                raise ValueError("Storage changed during initialization readback")
            result = dict(schema_version=1, source="real", input_origin="real",
                          epoch_id=manifest["epoch_id"], epoch_generation=manifest["generation"],
                          owner_manifest_sha256=digest(canonical(manifest)), collector=identity,
                          original_min_accepted_at=manifest["original_min_accepted_at"], expires_at=manifest["expires_at"],
                          initialized_at=current.isoformat(), storage=before, writers=writer_hashes(),
                          jar_sha256=manifest["jar"]["sha256"], initial=value,
                          initial_readback_sha256=digest(canonical(value)))
            write_json(self.path, result)
            return result

    def read(self, collector=None, *, now=None):
        current = now or datetime.now(UTC)
        manifest = frozen(self.epoch)
        if not self.path.exists():
            raise ValueError("Real writer initialization/readback has not been registered")
        value = bounded_json(self.path)
        same_keys(value, ("schema_version", "source", "input_origin", "epoch_id", "epoch_generation", "owner_manifest_sha256",
                          "collector", "original_min_accepted_at", "expires_at", "initialized_at", "storage", "writers",
                          "jar_sha256", "initial", "initial_readback_sha256"), "writer registration")
        expected = dict(schema_version=1, source="real", input_origin="real", epoch_id=manifest["epoch_id"],
                        epoch_generation=manifest["generation"], owner_manifest_sha256=digest(canonical(manifest)),
                        original_min_accepted_at=manifest["original_min_accepted_at"], expires_at=manifest["expires_at"],
                        jar_sha256=manifest["jar"]["sha256"], writers=writer_hashes())
        if any(value[k] != v for k, v in expected.items()) or type(value["schema_version"]) is not int:
            raise ValueError("Writer code, epoch generation or immutable window changed")
        actual = collector_identity(value["collector"])
        if collector is not None and actual != collector_identity(collector):
            raise ValueError("Writer registration belongs to a different collector")
        if (not timestamp(value["original_min_accepted_at"]) <= timestamp(value["initialized_at"]) <= current or
                not timestamp(value["original_min_accepted_at"]) <= current < timestamp(value["expires_at"]) or
                (self.epoch.directory / "retirement.json").exists()):
            raise ValueError("Writer registration expired or has been retired")
        validate_initial(value["initial"], manifest)
        if value["initial_readback_sha256"] != digest(canonical(value["initial"])):
            raise ValueError("Initialization readback was changed")
        return value

    def record_job(self, job_id, probe, *, now=None):
        current = now or datetime.now(UTC)
        if not re.fullmatch(r"[a-f0-9]{32}", job_id):
            raise ValueError("An actual Flink job ID is required")
        with publication_lock(self.epoch.directory):
            registration = self.read(now=current)
            if self.job_path.exists():
                raise ValueError("Job registration is immutable")
            if storage(self.epoch, running=True) != registration["storage"]:
                raise ValueError("Storage changed before the job readback")
            readback = probe.read_job(job_id)
            expected = dict(job_id=job_id, state="RUNNING", jar_sha256=registration["jar_sha256"],
                            parameters=expected_parameters(self.epoch.read(), registration["initial"]["kafka"]))
            if readback != expected:
                raise ValueError("Actual Flink job did not read back the frozen epoch/JAR/input bindings")
            result = dict(schema_version=1, source="real", registration_sha256=digest(self.path.read_bytes()),
                          observed_at=current.isoformat(), readback=readback, evidence_sha256=digest(canonical(readback)))
            write_json(self.job_path, result)
            return result

    def ready(self, collector=None, *, now=None):
        value = self.read(collector, now=now)
        if not self.job_path.exists():
            raise ValueError("Actual production Flink writer submission/readback has not been registered")
        job = bounded_json(self.job_path)
        same_keys(job, ("schema_version", "source", "registration_sha256", "observed_at", "readback", "evidence_sha256"), "writer job")
        expected = expected_parameters(self.epoch.read(), value["initial"]["kafka"])
        details = job["readback"]
        same_keys(details, ("job_id", "state", "jar_sha256", "parameters"), "job readback")
        if (job["schema_version"] != 1 or job["source"] != "real" or
                job["registration_sha256"] != digest(self.path.read_bytes()) or
                job["evidence_sha256"] != digest(canonical(details)) or details["parameters"] != expected or
                details["jar_sha256"] != value["jar_sha256"] or details["state"] != "RUNNING" or
                not re.fullmatch(r"[a-f0-9]{32}", details["job_id"]) or
                not timestamp(value["initialized_at"]) <= timestamp(job["observed_at"]) <= (now or datetime.now(UTC))):
            raise ValueError("Production writer readback is missing, changed or belongs to another epoch")
        return value

    def recovery(self, *, now=None):
        return RecoveryLedger(self).read(now=now)

    def current_job(self, *, now=None):
        self.ready(now=now)
        history = self.recovery(now=now)
        return next((item["payload"]["job_id"] for item in reversed(history) if item["action"] == "resumed"),
                    bounded_json(self.job_path)["readback"]["job_id"])

    def sync_ready(self, collector=None, *, now=None):
        value = self.ready(collector, now=now)
        history = self.recovery(now=now)
        if history and history[-1]["action"] != "resumed":
            raise ValueError("Writer pause/recovery is pending; synchronization remains closed")
        return value

    def complete_recovery(self, *, now=None):
        history = self.recovery(now=now)
        if history and history[-1]["action"] not in {"paused", "resumed"}:
            raise ValueError("Interrupted writer recovery blocks further data admission")
        return history

    def operation_lock(self):
        # One shared nonblocking OS lock covers the whole producer batch and
        # pause/resume. A busy sync must finish; never stop its engines first.
        folder = self.epoch.directory / "writer-operation"
        if folder.resolve() != folder.absolute():
            raise ValueError("Writer operation directory cannot traverse links")
        return publication_lock(folder)


def checkpoint_metadata(value, job_id, registration, now):
    """Only the pinned Flink filesystem checkpoint inside this epoch volume."""
    same_keys(value, ("job_id", "id", "external_path", "trigger_timestamp", "latest_ack_timestamp"), "checkpoint metadata")
    if (value["job_id"] != job_id or type(value["id"]) is not int or value["id"] <= 0 or
            value["external_path"] != f"file:///checkpoints/{job_id}/chk-{value['id']}" or
            any(type(value[k]) is not int for k in ("trigger_timestamp", "latest_ack_timestamp")) or
            not int(timestamp(registration["initialized_at"]).timestamp() * 1000) <= value["trigger_timestamp"] <=
                value["latest_ack_timestamp"] <= int(now.timestamp() * 1000)):
        raise ValueError("Checkpoint must belong to the actual job and original initialized epoch")
    return value


def checkpoint_files(value, jobs, checkpoint):
    """Metadata only: hash every owned checkpoint file including shared state."""
    if not isinstance(value, list) or not 1 <= len(value) <= 512:
        raise ValueError("Checkpoint file inventory is missing or exceeds the bounded small-data limit")
    previous, total = "", 0
    for item in value:
        same_keys(item, ("path", "bytes", "sha256"), "checkpoint file hash")
        path = item["path"]
        if (not isinstance(path, str) or not re.fullmatch(r"[a-f0-9]{32}/(?:[A-Za-z0-9_-]+/)*[A-Za-z0-9_.-]+", path)
                or any(p in {".", ".."} for p in path.split("/")) or path.split("/")[0] not in jobs
                or path <= previous or type(item["bytes"]) is not int or item["bytes"] < 0
                or not isinstance(item["sha256"], str) or not re.fullmatch(r"[a-f0-9]{64}", item["sha256"])):
            raise ValueError("Checkpoint inventory contains unregistered paths or invalid file hashes")
        previous, total = path, total + item["bytes"]
    if total > 128 * 1024 * 1024 or not any(v["path"] == f"{checkpoint['job_id']}/chk-{checkpoint['id']}/_metadata" for v in value):
        raise ValueError("Checkpoint metadata is missing or exceeds 128 MiB bounded recovery capacity")
    return value


class RecoveryLedger:
    """Append-only receipts anchored to immutable initial registration/job bytes.

    This is crash/tamper detection, not a signature against a filesystem owner.
    APIs accepting actual adapters create it; no CLI imports supplied receipts.
    An incomplete final append or interrupted operation always closes sync.
    """
    def __init__(self, registry):
        self.registry = registry
        self.directory = registry.epoch.directory / "writer-recovery"

    def read(self, *, now=None):
        now = now or datetime.now(UTC)
        registration = self.registry.ready(now=now)
        if self.directory.resolve() != self.directory.absolute():
            raise ValueError("Recovery ledger cannot traverse links")
        if not self.directory.exists():
            return []
        names = {p.name for p in self.directory.iterdir()}
        if not names:
            return []
        if "head.json" not in names or len(names) > 1001:
            raise ValueError("Incomplete recovery ledger; synchronization remains closed")
        head = bounded_json(self.directory / "head.json")
        same_keys(head, ("sequence", "sha256"), "recovery head")
        if type(head["sequence"]) is not int or not 1 <= head["sequence"] <= 1000 or names != {"head.json"} | {
                f"{i:06d}.json" for i in range(1, head["sequence"] + 1)}:
            raise ValueError("Recovery ledger has missing, extra or interrupted records")
        history, previous = [], digest(self.registry.job_path.read_bytes())
        jobs = {bounded_json(self.registry.job_path)["readback"]["job_id"]}
        current_job, state, checkpoint, files = next(iter(jobs)), "resumed", None, None
        observed = timestamp(bounded_json(self.registry.job_path)["observed_at"])
        for sequence in range(1, head["sequence"] + 1):
            entry = bounded_json(self.directory / f"{sequence:06d}.json")
            same_keys(entry, ("schema_version", "source", "sequence", "previous_sha256", "registration_sha256",
                             "epoch_generation", "expires_at", "observed_at", "action", "payload"), "recovery record")
            if (entry["schema_version"] != 1 or type(entry["schema_version"]) is not int or entry["source"] != "real"
                    or entry["sequence"] != sequence or entry["previous_sha256"] != previous
                    or entry["registration_sha256"] != digest(self.registry.path.read_bytes())
                    or entry["epoch_generation"] != registration["epoch_generation"]
                    or entry["expires_at"] != registration["expires_at"]
                    or not observed <= timestamp(entry["observed_at"]) <= now):
                raise ValueError("Recovery chain or immutable epoch deadline changed")
            observed = timestamp(entry["observed_at"])
            action, payload = entry["action"], entry["payload"]
            if action == "pause_requested" and state == "resumed":
                same_keys(payload, ("job_id",), "pause request")
                if payload["job_id"] != current_job:
                    raise ValueError("Cannot pause an unregistered JobID")
            elif action == "paused" and state == "pause_requested":
                same_keys(payload, ("job_id", "checkpoint", "files", "files_sha256", "storage_sha256", "state"), "paused checkpoint")
                checkpoint = checkpoint_metadata(payload["checkpoint"], current_job, registration, observed)
                files = checkpoint_files(payload["files"], jobs, checkpoint)
                if (payload["job_id"] != current_job or payload["state"] != "CANCELED"
                        or payload["files_sha256"] != digest(canonical(files))
                        or payload["storage_sha256"] != digest(canonical(registration["storage"]))):
                    raise ValueError("Paused checkpoint storage or file hash changed")
            elif action == "resume_requested" and state == "paused":
                same_keys(payload, ("job_id", "checkpoint_sha256", "restore_mode"), "resume request")
                if (payload["job_id"] != current_job or payload["checkpoint_sha256"] != digest(canonical(checkpoint))
                        or payload["restore_mode"] != "NO_CLAIM"):
                    raise ValueError("Resume request changed the exact checkpoint or restore mode")
            elif action == "resume_acknowledged" and state == "resume_requested":
                same_keys(payload, ("job_id", "from_job_id", "checkpoint_sha256", "environment_sha256"), "restore acknowledgement")
                if (not re.fullmatch(r"[a-f0-9]{32}", payload["job_id"]) or payload["job_id"] in jobs
                        or payload["from_job_id"] != current_job
                        or payload["checkpoint_sha256"] != digest(canonical(checkpoint))
                        or not re.fullmatch(r"[a-f0-9]{64}", payload["environment_sha256"])):
                    raise ValueError("Restore acknowledgement is outside the registered submission")
                current_job = payload["job_id"]
                jobs.add(current_job)
            elif action == "resumed" and state == "resume_acknowledged":
                same_keys(payload, ("job_id", "readback", "restored"), "resumed job")
                expected = dict(job_id=current_job, state="RUNNING", jar_sha256=registration["jar_sha256"],
                                parameters=expected_parameters(self.registry.epoch.read(), registration["initial"]["kafka"]))
                restored = payload["restored"]
                same_keys(restored, ("id", "external_path", "restore_timestamp", "is_savepoint"), "actual restored checkpoint")
                if (payload["job_id"] != current_job or payload["readback"] != expected or restored["id"] != checkpoint["id"]
                        or restored["external_path"] != checkpoint["external_path"]
                        or type(restored["restore_timestamp"]) is not int or type(restored["is_savepoint"]) is not bool
                        or not int(timestamp(history[-2]["observed_at"]).timestamp() * 1000) <= restored["restore_timestamp"] <= int(observed.timestamp() * 1000)):
                    raise ValueError("Actual recovered job did not restore the registered checkpoint")
            else:
                raise ValueError("Invalid recovery state transition")
            state, previous = action, digest(canonical(entry))
            history.append(entry)
        if head["sha256"] != previous:
            raise ValueError("Recovery head does not match append-only chain")
        return history

    def append(self, action, payload, *, now=None):
        # Caller holds registry.operation_lock throughout actual I/O. The short
        # registry lock prevents concurrent metadata mutation as a second guard.
        now = now or datetime.now(UTC)
        with publication_lock(self.registry.epoch.directory):
            registration = self.registry.ready(now=now)
            history = self.read(now=now)
            sequence = len(history) + 1
            value = dict(schema_version=1, source="real", sequence=sequence,
                previous_sha256=digest(canonical(history[-1])) if history else digest(self.registry.job_path.read_bytes()),
                registration_sha256=digest(self.registry.path.read_bytes()), epoch_generation=registration["epoch_generation"],
                expires_at=registration["expires_at"], observed_at=now.isoformat(), action=action, payload=payload)
            self.directory.mkdir(mode=0o700, exist_ok=True)
            path = self.directory / f"{sequence:06d}.json"
            if path.exists():
                raise ValueError("Recovery records cannot be overwritten")
            write_json(path, value)
            write_json(self.directory / "head.json", dict(sequence=sequence, sha256=digest(canonical(value))))
            self.read(now=now)  # Malformed/adaptor failure remains fail closed.
            return value


class WindowGuard:
    """Per-row original-time admission for both fresh sync and durable replay."""
    def __init__(self, registry, source_file, *, clock=None):
        self.registry, self.source_file = registry, Path(source_file)
        self.clock = clock or (lambda: datetime.now(UTC))
        registry.sync_ready(now=self.clock())

    def __call__(self, row):
        current = self.clock()
        value = self.registry.sync_ready(collector_identity(bounded_json(self.source_file)), now=current)
        accepted = timestamp(row["accepted_at"])
        if (row["source"] != "real" or accepted < timestamp(value["original_min_accepted_at"]) or
                accepted > current or accepted >= timestamp(value["expires_at"])):
            raise ValueError("Input acceptance time is outside the immutable real writer epoch")


def bind_ods(registration, snapshot):
    identity = snapshot["identity"]
    names = registration["initial"]["kafka"]["identity"]
    if (snapshot["source"] != "real" or identity.get("collector") != registration["collector"] or
            identity.get("cluster_id") != names["cluster_id"] or
            identity.get("topic_ids") != {topic: identifier for topic, identifier in names["topic_ids"].items()
                                        if topic.endswith(".events.v1")}):
        raise ValueError("ODS collector/Kafka generation differs from initialized epoch")
    input_topic = next(name for name in names["topic_ids"] if name.endswith(".events.v1"))
    if (set(snapshot["offsets"]) != {input_topic + ":0"} or
            type(snapshot["offsets"][input_topic + ":0"]) is not int or snapshot["offsets"][input_topic + ":0"] < 0):
        raise ValueError("ODS must use the initialized single input partition")
    lower = timestamp(registration["original_min_accepted_at"])
    deadline = timestamp(registration["expires_at"])
    for batch in snapshot["batches"]:
        if (timestamp(batch["original_min_accepted_at"]) < lower or
                timestamp(batch["original_min_accepted_at"]) >= deadline or
                timestamp(batch["expires_at"]) != timestamp(batch["original_min_accepted_at"]) + timedelta(days=7)):
            raise ValueError("ODS copied input predates the physical epoch window")


def backend_resources(registration):
    """Every initialized engine copy is registered, including side outputs."""
    manifest_lane = registration["epoch_id"].replace("-", "_")
    origin = registration["original_min_accepted_at"]
    def entry(kind):
        return dict(kind=kind, original_min_accepted_at=origin,
                    expires_at=(timestamp(origin) + timedelta(days=RETENTION_DAYS[kind])).isoformat())
    return dict(kafka={name: entry("raw") for name in registration["initial"]["kafka"]["identity"]["topic_ids"]},
                doris={"snow_real_" + manifest_lane + "." + name: entry("raw" if name == "events_realtime" else "aggregate") for name in TABLES},
                checkpoint={"docker-volume://snow-real-" + registration["epoch_id"] + "-" + role: entry("raw")
                            for role in ("checkpoints", "flink-state")})


class StoppedStorage:
    """Explicit quiescent receipt; proves no expired physical epoch, not SQL rows."""
    def __init__(self, registry, snapshot):
        self.registry, self.snapshot = registry, snapshot

    def check(self, *, now=None):
        current = now or datetime.now(UTC)
        epoch = self.registry.epoch
        # Real wall-clock cleanup. A stopped backend never exempts expired data.
        expire_due(epoch.directory.parent, epoch.docker)
        value = self.registry.ready(now=current)
        self.registry.complete_recovery(now=current)
        bind_ods(value, self.snapshot)
        if storage(epoch, running=False) != value["storage"]:
            raise ValueError("Stopped physical objects differ from initialized storage")
        current = max(current, datetime.now(UTC))
        self.registry.ready(now=current)
        self.registry.complete_recovery(now=current)
        return dict(schema_version=1, source="real", input_origin="real", verification="stopped_storage_unexpired",
                    epoch_id=value["epoch_id"], epoch_generation=value["epoch_generation"],
                    collector=value["collector"], owner_manifest_sha256=value["owner_manifest_sha256"],
                    registration_sha256=digest(self.registry.path.read_bytes()),
                    writer_job_sha256=digest(self.registry.job_path.read_bytes()),
                    original_min_accepted_at=value["original_min_accepted_at"], expires_at=value["expires_at"],
                    checked_at=current.isoformat(), storage_sha256=digest(canonical(value["storage"])),
                    sql_cleanup_verified=False, kafka_records_scanned=False, physical_deletion_due=False)

    def adapters(self):
        return {name: StoppedBackend(self, name) for name in ("kafka", "doris", "checkpoint")}


class StoppedBackend:
    def __init__(self, stopped, name):
        self.stopped, self.name = stopped, name

    def verify_stopped(self, resources, now):
        evidence = self.stopped.check(now=now)
        registration = self.stopped.registry.ready(now=now)
        if resources != backend_resources(registration)[self.name]:
            raise ValueError("Stopped admission requires every exact initialized backend resource")
        return dict(backend=self.name, verification="stopped_storage_unexpired",
                    resources_sha256=digest(canonical(resources)), checked_at=evidence["checked_at"],
                    evidence=evidence, evidence_sha256=digest(canonical(evidence)),
                    next_expiry=evidence["expires_at"])


def validate_stopped_receipt(receipt, name, resources, owner, snapshot, now):
    """Called only after the in-process stopped verifier's actual Docker reads."""
    same_keys(receipt, ("backend", "verification", "resources_sha256", "checked_at", "evidence", "evidence_sha256", "next_expiry"), "stopped receipt")
    evidence = receipt["evidence"]
    same_keys(evidence, ("schema_version", "source", "input_origin", "verification", "epoch_id", "epoch_generation", "collector",
                         "owner_manifest_sha256", "registration_sha256", "writer_job_sha256", "original_min_accepted_at", "expires_at",
                         "checked_at", "storage_sha256", "sql_cleanup_verified", "kafka_records_scanned", "physical_deletion_due"), "stopped evidence")
    if (receipt["backend"] != name or receipt["verification"] != "stopped_storage_unexpired" or
            receipt["resources_sha256"] != digest(canonical(resources)) or
            receipt["evidence_sha256"] != digest(canonical(evidence)) or
            evidence["verification"] != receipt["verification"] or evidence["source"] != "real" or
            evidence["input_origin"] != "real" or evidence["schema_version"] != 1 or
            evidence["collector"] != snapshot["identity"]["collector"] or
            any(evidence["collector"][key] != owner[key] for key in ("instance_id", "generation")) or
            any(evidence[key] is not False for key in ("sql_cleanup_verified", "kafka_records_scanned", "physical_deletion_due")) or
            receipt["checked_at"] != evidence["checked_at"] or not now <= timestamp(receipt["checked_at"]) <= now + timedelta(minutes=10) or
            receipt["next_expiry"] != evidence["expires_at"] or timestamp(evidence["expires_at"]) <= now or
            timestamp(evidence["expires_at"]) != timestamp(evidence["original_min_accepted_at"]) + timedelta(days=7)):
        raise ValueError("Invalid stopped-storage evidence or fixed deadline")
    for entry in resources.values():
        if (timestamp(entry["original_min_accepted_at"]) < timestamp(evidence["original_min_accepted_at"]) or
                timestamp(entry["expires_at"]) < timestamp(evidence["expires_at"])):
            raise ValueError("A stopped copy expires before the physical epoch deadline")
    return receipt


def validate_doris_write(package, registry, collector, *, now=None):
    """Future production publisher must call this before each SQL mutation."""
    value = registry.ready(collector, now=now)
    registry.complete_recovery(now=now)
    actual = storage(registry.epoch, running=None)
    if actual != value["storage"]:
        raise ValueError("Doris physical storage changed")
    manifest = registry.epoch.read()
    containers, _ = registry.epoch._inspect(manifest)
    if any(not containers[manifest["containers"][role]]["running"] for role in ("doris-fe", "doris-be")):
        raise ValueError("Registered Doris storage must be running for writes")
    from .publication import validate
    from .real_behavior import HK
    from .real_publication import validate_manifest
    manifest, rows, _, _, _ = validate(package)
    validate_manifest(manifest, "daily")
    snapshot = manifest["input_snapshot"]
    topic = topic_names(registry.epoch.read())[0]
    if (manifest["source"] != "real" or snapshot["collector"] != value["collector"] or
            set(snapshot["offsets"]) != {topic + ":0"} or
            "/kafka/" + value["epoch_id"] + "/snapshots/" not in manifest["input"]):
        raise ValueError("Doris input collector/topic/lane differs from its initialized epoch")
    # Business date is distinct from collector accepted_at. Late arrivals may
    # legitimately describe a previous day; only their actual aggregate expiry
    # must not precede physical disposal of the stopped epoch.
    aggregate_expiry = datetime.combine(date.fromisoformat(manifest["date_from"]), time(), HK) + timedelta(days=90)
    if aggregate_expiry < timestamp(value["expires_at"]):
        raise ValueError("An aggregate expires before the physical epoch deadline")
    return value
