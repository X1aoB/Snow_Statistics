"""Bounded synthetic collector/Source fixture for actual HDFS/Spark/Hive tests.

The Source adapter is NOT Kafka. It never certifies a Kafka, Doris or checkpoint
backend, accepts production config, imports an old registry, or manufactures an
engine result. All exported event bodies must equal this module's fixed generator.
"""
import base64
import json
import re
import secrets
import socket
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

from .contracts import Event
from .io import digest, write_json
from .landing import acknowledge, capture, checked_receipt, collector_identity, land
from .lifecycle import RealLifecycle, timestamp
from .model import daily_metrics
from .publication import canonical
from .real_lab import hdfs_context, metadata_paths, validate_config, validate_job
from .real_remote_lifecycle import RealRemoteLifecycle
from .simulator import generate
from .source_cursor import validate_status

ORIGIN = "synthetic fixtures"
ADAPTER = "deterministic_fixture_source_not_kafka"
MAX_ROWS = 56
MAX_BYTES = 1024**2


def lane_name(lane):
    if not isinstance(lane, str) or not re.fullmatch(r"fixture-lake-[a-z0-9]{2,10}", lane):
        raise ValueError("Only a fresh fixture-lake-* lane is allowed")
    return lane


def locations(root, lane):
    lane_name(lane)
    root = Path(root).absolute()
    values = dict(directory=root / "runtime/real/lake-fixture" / lane,
                  config=root / "runtime/real/config" / (lane + ".json"),
                  ods=root / "runtime/real/ods" / lane,
                  registry=root / "runtime/real/lifecycle" / lane)
    if root.resolve() != root or any(path.resolve() != path for path in values.values()):
        raise ValueError("Fixture resources cannot traverse a link")
    return values


def fixture_config(value, lane):
    validate_config(value)
    if (value["lane"] != lane_name(lane) or value["input_origin"] != ORIGIN
            or value["transport_node"] != "snow-analysis" or value["tunnel"] is not None
            or value["backend_config_file"] is not None or value["doris_config_file"] is not None
            or value["reader_token_file"] != "runtime/real/secrets/" + lane + ".token"):
        raise ValueError("Fixture tool rejects production connectivity and backend credentials")
    return value


def events(start):
    # Generate directly for this fixture. Never rewrite imported business rows.
    rows = generate(seed=71819, users=2, start=timestamp(start))["events"]
    return [json.loads(Event.model_validate(row["event"]).model_dump_json(exclude_none=True)) for row in rows]


def read_fixture(root, lane):
    paths = locations(root, lane)
    config, value = fixture_metadata(root, lane)

    local = RealLifecycle(paths["directory"] / "data")
    local.cleanup()
    payload_path = local.readable("envelopes.json")
    if payload_path.stat().st_size > MAX_BYTES:
        raise ValueError("Fixture payload exceeds its byte budget")
    payload = payload_path.read_bytes()
    if digest(payload) != value["envelopes_sha256"]:
        raise ValueError("Fixture input size/checksum differs")
    rows = json.loads(payload)
    expected = events(value["event_start"])
    if not 1 <= len(rows) == len(expected) <= MAX_ROWS:
        raise ValueError("Fixture row bound differs")
    for index, (row, event) in enumerate(zip(rows, expected, strict=True), 1):
        if (set(row) != {"seq", "source", "accepted_at", "event"} or row["source"] != "real"
                or row["seq"] != index or row["event"] != event
                or not timestamp(value["created_at"]) <= timestamp(row["accepted_at"]) <= timestamp(value["cutoff"])):
            raise ValueError("Only this newly generated collector input may enter the fixture")
    return paths, config, value, rows


def fixture_metadata(root, lane):
    from .real_lab import read_json, secret_file
    paths = locations(root, lane)
    value = read_json(paths["directory"] / "manifest.json")
    config = fixture_config(read_json(secret_file(Path(root), paths["config"].relative_to(root).as_posix())), lane)
    if (value.get("schema_version") != 1 or value.get("input_origin") != ORIGIN
            or value.get("transport_adapter") != ADAPTER or value.get("lane") != lane
            or value.get("config_sha256") != digest(canonical(config))
            or value.get("kafka_engine_tested") is not False):
        raise ValueError("Fixture manifest changed source or connectivity")
    collector_identity(value["collector"])
    return config, value


class FixtureSource:
    """Explicit durable test adapter; no broker connection or broker claim."""
    def __init__(self, root, lane):
        self.paths, self.config, self.manifest, self.rows = read_fixture(root, lane)
        self.topic = "snow.real." + lane.replace("-", "_") + ".events.v1"
        self.key = self.topic + ":0"

    def identity(self):
        generation = self.manifest["collector"]["generation"]
        return dict(cluster_id="fixture-adapter-" + generation, group="snow-ods-" + self.config["lane"],
                    topic_ids={self.topic: "fixture-adapter-" + self.manifest["envelopes_sha256"][:32]},
                    collector=self.manifest["collector"])

    def bounds(self):
        return {self.key: (0, len(self.rows))}

    def committed(self, key):
        if key != self.key:
            raise ValueError("Fixture partition escaped")
        path = self.paths["directory"] / "adapter-commit.json"
        if not path.exists():
            return None
        value = json.loads(path.read_bytes())
        if value != {"transport_adapter": ADAPTER, "identity": self.identity(), "offsets": {key: len(self.rows)}}:
            raise ValueError("Fixture simulated commit metadata changed")
        return value["offsets"][key]

    def read(self, key, start, end):
        if key != self.key or start != 0 or end != len(self.rows):
            raise ValueError("Only the complete bounded fixture prefix is readable")
        for offset, row in enumerate(self.rows):
            yield dict(topic=self.topic, partition=0, offset=offset,
                       timestamp_ms=int(timestamp(row["accepted_at"]).timestamp() * 1000), key_b64=None,
                       value_b64=base64.b64encode(canonical(row)).decode("ascii"), headers=[])

    def commit(self, ends):
        if ends != {self.key: len(self.rows)}:
            raise ValueError("Fixture simulated ACK escaped the generated prefix")
        write_json(self.paths["directory"] / "adapter-commit.json",
                   dict(transport_adapter=ADAPTER, identity=self.identity(), offsets=ends))


def initialize(root, lane, nodes):
    # The 768 MiB analysis node also hosts a DataNode during later phases.
    # Pure metadata/landing reads must not initialize the HTTP server stack.
    import httpx
    import uvicorn

    from .api import create_app
    from .config import Settings

    paths = locations(root, lane)
    if any(path.exists() for path in paths.values()):
        raise ValueError("Fixture initialization requires entirely fresh owned paths")
    root = Path(root).absolute()
    token_path = root / "runtime/real/secrets" / (lane + ".token")
    if token_path.exists() or token_path.resolve() != token_path:
        raise ValueError("Fixture token path must be fresh and owned")
    created = datetime.now(UTC)
    start = datetime.combine(created.date() - timedelta(days=4), datetime.min.time(), UTC)
    rows = events(start.isoformat())
    token, server_token = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    config = fixture_config(dict(schema_version=1, source="real", input_origin=ORIGIN, lane=lane,
        transport_node="snow-analysis", nodes=nodes, collector_url="http://127.0.0.1:" + str(listener.getsockname()[1]),
        reader_token_file=token_path.relative_to(root).as_posix(), tunnel=None, backend_config_file=None,
        doris_config_file=None), lane)
    paths["directory"].mkdir(parents=True, mode=0o700)
    write_json(paths["directory"] / "intent.json", dict(input_origin=ORIGIN, lane=lane, created_at=created.isoformat(),
                                                        transport_adapter=ADAPTER, event_start=start.isoformat()))
    local = RealLifecycle(paths["directory"] / "data")
    local.initialize()
    for name in ("collector.sqlite", "collector.sqlite-wal", "collector.sqlite-shm", "envelopes.json"):
        local.register(name, "raw", created.isoformat())
    settings = Settings(mode="full", source="real", db=local.path("collector.sqlite"), reader_token=token,
        server_token=server_token, aggregate_interval=0.05, allowed_paths=frozenset({"/"}),
        allowed_characters=frozenset({"sample_character"}))
    server = uvicorn.Server(uvicorn.Config(create_app(settings), access_log=False, log_level="error"))
    worker = threading.Thread(target=server.run, kwargs={"sockets": [listener]}, daemon=True)
    worker.start()
    try:
        deadline = time.monotonic() + 15
        while not server.started:
            if not worker.is_alive() or time.monotonic() > deadline:
                raise TimeoutError("Independent fixture collector did not start")
            time.sleep(0.05)
        with httpx.Client(base_url=config["collector_url"], timeout=5, trust_env=False) as client:
            for index in range(0, len(rows), 10):
                response = client.post("/analytics/v1/events", json={"events": rows[index:index + 10]},
                    headers={"Authorization": "Bearer " + server_token, "Origin": "https://xiaob.dev"})
                response.raise_for_status()
            auth = {"Authorization": "Bearer " + token}
            response = client.get("/analytics/private/v1/events", headers=auth)
            response.raise_for_status()
            envelopes = response.json()["events"]
            deadline = time.monotonic() + 15
            while True:
                response = client.get("/analytics/private/v1/status", headers=auth)
                response.raise_for_status()
                status = response.json()
                identity = validate_status(status)
                if status["latest_accepted_seq"] == status["aggregate_cursor"] == len(rows):
                    break
                if time.monotonic() > deadline:
                    raise TimeoutError("Fixture collector aggregation did not finish")
                time.sleep(0.05)
            response = client.get("/analytics/private/v1/summary.json", headers=auth)
            response.raise_for_status()
            expected = sorted(daily_metrics(envelopes), key=canonical)
            lite = sorted([dict(source="real", **{k: row[k] for k in ("app", "date", "pv", "uv", "requests", "successes")})
                           for row in response.json()["daily"]], key=canonical)
            if lite != expected:
                raise ValueError("Fixture collector differs from independent daily metric oracle")
    finally:
        server.should_exit = True
        worker.join(15)
        listener.close()
        if worker.is_alive():
            raise RuntimeError("Fixture collector failed to stop")
    cutoff = datetime.now(UTC).isoformat()
    payload = canonical(envelopes)
    if len(payload) > MAX_BYTES:
        raise ValueError("Fixture payload exceeds its byte budget")
    local.path("envelopes.json").write_bytes(payload)
    token_path.parent.mkdir(parents=True, exist_ok=True)
    token_path.write_text(token)
    token_path.chmod(0o600)
    write_json(paths["config"], config)
    paths["config"].chmod(0o600)
    value = dict(schema_version=1, input_origin=ORIGIN, lane=lane, transport_adapter=ADAPTER,
        kafka_engine_tested=False, collector_http_tested=True, created_at=created.isoformat(), event_start=start.isoformat(),
        cutoff=cutoff, collector=identity, config_sha256=digest(canonical(config)), envelopes_sha256=digest(payload),
        expected_daily=expected, date_from=min(row["date"] for row in expected), date_to=max(row["date"] for row in expected))
    write_json(paths["directory"] / "manifest.json", value)
    source = FixtureSource(root, lane)
    batch = capture(paths["ods"], source, provenance="real", event_lane=lane.replace("-", "_"),
                    max_records=MAX_ROWS, max_bytes=MAX_BYTES)
    return dict(input_origin=ORIGIN, transport_adapter=ADAPTER, kafka_engine_tested=False, batch_id=batch)


def land_and_reserve(root, lane):
    root = Path(root).absolute()
    paths, config, fixture, rows = read_fixture(root, lane)
    source = FixtureSource(root, lane)
    with hdfs_context(config) as (sink, hdfs):
        if (paths["ods"] / "pending.json").exists():
            land(paths["ods"], sink)  # Actual hash/BlockLocation two-replica checks.
            acknowledge(paths["ods"], source)  # Explicit fixture adapter ACK only.
        state = json.loads((paths["ods"] / "state.json").read_bytes())
        checked_receipt(state)
        if (state["identity"] != source.identity() or state["offsets"] != {source.key: len(rows)}
                or source.committed(source.key) != len(rows)):
            raise ValueError("Committed fixture head differs from its actual capture")
        prefix = "hdfs://" + config["nodes"]["snow-control"] + ":9000/snow/"
        run_id = lane + "-r1"
        job = validate_job(dict(run_id=run_id, kind="daily", source="real", input=state["input"],
            warehouse_root=prefix + "warehouse/real/" + lane, auxiliary_root=prefix + "auxiliary/real/" + lane,
            date_from=fixture["date_from"], date_to=fixture["date_to"], cutoff=fixture["cutoff"],
            coverage_file=run_id + ".json", auxiliary_file=None, permit_file=run_id + ".json", register_hive=False), config, run_id)
        original = min(row["accepted_at"] for row in rows)
        coverage = fixture["collector"] | dict(continuous_from=original, through=fixture["cutoff"], gaps=[])
        metadata = metadata_paths(config, run_id)
        for name, content in (("job", job), ("coverage", coverage)):
            target = root / metadata[name]
            if target.exists() and json.loads(target.read_bytes()) != content:
                raise ValueError("Fixture job identity/cutoff cannot be replaced")
            write_json(target, content)
        manager = RealRemoteLifecycle(paths["registry"])
        if not manager.owner.exists():
            manager.initialize(job["warehouse_root"], job["auxiliary_root"], state["root"],
                               fixture["collector"]["instance_id"], fixture["collector"]["generation"])
        owner, registered = manager._read()
        if any(owner[key] != fixture["collector"][key] for key in ("instance_id", "generation")):
            raise ValueError("Fixture cannot adopt an existing collector registry")
        if any(registered["backends"][name]["state"] != "not_initialized" for name in ("kafka", "doris", "checkpoint")):
            raise ValueError("Fixture adapter cannot certify actual engine backends")
        manager.reserve_job(job, original, original)
        permit = manager.issue_permit(job, root / metadata["coverage"], None, root / metadata["permit"],
                                      hdfs, paths["ods"], sink)
    return dict(input_origin=ORIGIN, transport_adapter=ADAPTER, kafka_engine_tested=False, run_id=run_id,
                hdfs_verified=True, permit_expires_at=permit["expires_at"])


def transfer(root, lane, phase):
    """Normal managed library validation; never the production CLI exception."""
    from .real_publication import read_real_release
    from .real_transfer import accept, export, paths, read, reserve, validate_payload
    root = Path(root).absolute()
    config, fixture = fixture_metadata(root, lane)
    run_id = lane + "-r1"
    relative = paths(lane, run_id)
    if phase == "export":
        release = read_real_release(root / "runtime/real/publication")
        if (release["run_id"] != run_id
                or release["daily"]["manifest"]["input_snapshot"]["collector"] != fixture["collector"]
                or sorted(release["daily"]["daily"], key=canonical) != sorted(fixture["expected_daily"], key=canonical)):
            raise ValueError("Actual Spark release differs from this collector's independent oracle")
        result = export(root, config, run_id)
    elif phase == "reserve":
        incoming = root / relative["incoming_manifest"]
        result = json.loads(read(incoming, 65536))
        if result["collector"] != fixture["collector"]:
            raise ValueError("Transferred fixture collector differs")
        reserve(root, config, run_id, result)
    elif phase == "accept":
        manifest = json.loads(read(root / relative["manifest"], 65536))
        payload = read(root / relative["incoming_pair"])
        candidate = validate_payload(payload, manifest, config, run_id)
        if (manifest["collector"] != fixture["collector"]
                or sorted(candidate["daily"]["daily"], key=canonical) != sorted(fixture["expected_daily"], key=canonical)):
            raise ValueError("Transferred fixture output differs from its independent oracle")
        result = accept(root, config, run_id, publish=True)
        release = read_real_release(root / relative["published"])
        if (result["collector"] != fixture["collector"]
                or sorted(release["daily"]["daily"], key=canonical) != sorted(fixture["expected_daily"], key=canonical)):
            raise ValueError("Transferred fixture output differs from its independent oracle")
    else:
        raise ValueError("Unknown fixture aggregate transfer phase")
    return dict(input_origin=ORIGIN, transport_adapter=ADAPTER, kafka_engine_tested=False,
                run_id=run_id, phase=phase, aggregate_sha256=result["sha256"])
