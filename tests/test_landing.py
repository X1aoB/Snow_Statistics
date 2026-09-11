import base64
import copy
import importlib.util
import json
from pathlib import Path

import pytest

from snow_statistics.io import digest, write_json
from snow_statistics.landing import TOPICS, acknowledge, capture, land, pending
from snow_statistics.publication import canonical
from snow_statistics.simulator import generate


def record(value, offset, topic=TOPICS[0]):
    return dict(topic=topic, partition=0, offset=offset, timestamp_ms=0, key_b64=None,
                value_b64=None if value is None else base64.b64encode(canonical(value)).decode(), headers=[])


class Broker:
    def __init__(self):
        self.id = dict(cluster_id="cluster", topic_ids={t: t for t in TOPICS}, group="snow-ods-test")
        self.rows = {t + ":0": [] for t in TOPICS}
        self.rows[TOPICS[0] + ":0"] = [record(r, i) for i, r in enumerate(generate(users=1)["events"][:3])]
        self.offsets, self.earliest = {}, 0

    def identity(self):
        return copy.deepcopy(self.id)

    def bounds(self):
        return {k: (self.earliest, len(v)) for k, v in self.rows.items()}

    def committed(self, key):
        return self.offsets.get(key)

    def read(self, key, start, end):
        return iter(self.rows[key][start:end])

    def commit(self, ends):
        self.offsets.update(ends)


class Sink:
    root = "hdfs://snow-control:9000/snow/ods/synthetic/kafka/test"

    def __init__(self):
        self.directories = {}

    def put_directory(self, path, files):
        if path in self.directories and self.directories[path] != files:
            raise ValueError("HDFS conflict")
        self.directories[path] = files


def test_crashes_after_hdfs_commit_and_kafka_ack_are_idempotent(tmp_path):
    broker, sink = Broker(), Sink()
    batch = capture(tmp_path, broker)
    assert broker.offsets == {}
    with pytest.raises(ValueError, match="not been verified"):
        acknowledge(tmp_path, broker)
    with pytest.raises(RuntimeError, match="HDFS batch"):
        land(tmp_path, sink, fail_after_batch=True)
    assert len(sink.directories) == 1 and broker.offsets == {}
    broker.rows[TOPICS[0] + ":0"].append(record(generate(users=1)["events"][3], 3))
    assert capture(tmp_path, broker) == batch  # Pending input is frozen despite new messages.
    receipt = land(tmp_path, sink)
    assert receipt == land(tmp_path, sink) and len(sink.directories) == 2
    with pytest.raises(RuntimeError, match="Kafka ACK"):
        acknowledge(tmp_path, broker, fail_after_commit=True)
    assert not (tmp_path / "state.json").exists() and broker.offsets[TOPICS[0] + ":0"] == 3
    assert acknowledge(tmp_path, broker) == receipt
    assert capture(tmp_path, broker) != batch
    next_receipt = land(tmp_path, sink)
    assert len(next_receipt["batches"]) == 2
    acknowledge(tmp_path, broker)
    assert capture(tmp_path, broker) is None


def test_cdc_tombstone_delete_and_quarantine_reconcile(tmp_path):
    broker = Broker()
    row = dict(id="content-2", version=2, updated_at=1767225600000)
    value = dict(op="d", before=row, after=None, source=dict(db="snow_ops", table="contents"), transaction={"id": "tx1"})
    topic = "snow.synthetic.cdc.snow_ops.contents"
    broker.rows[topic + ":0"] = [record(value, 0, topic), record(None, 1, topic), record({"bad": "private diagnostic"}, 2, topic)]
    capture(tmp_path, broker)
    token, manifest = pending(tmp_path)
    counts = manifest["counts"]
    assert counts == dict(raw=6, events=3, changes=1, quarantine=1, tombstones=1)
    folder = tmp_path / "batches" / token
    change = json.loads((folder / "changes.jsonl").read_bytes())
    assert change["op"] == "d" and change["after"] is None and change["transaction"]["id"] == "tx1"
    assert "private diagnostic" not in (folder / "quarantine.jsonl").read_text()
    raw = [json.loads(line) for line in (folder / "raw.jsonl").read_bytes().splitlines()]
    assert any(r["value_b64"] is None for r in raw)
    receipt = land(tmp_path, Sink())
    acknowledge(tmp_path, broker)  # Raw is durably retained; downstream quality gate still refuses it.
    with pytest.raises(ValueError, match="quarantined"):
        resolve(receipt)


@pytest.mark.parametrize("failure", ["gap", "partial", "retention", "foreign_group", "bytes"])
def test_capture_failure_never_acknowledges_or_publishes(tmp_path, failure):
    broker = Broker()
    if failure == "gap":
        broker.rows[TOPICS[0] + ":0"][1]["offset"] = 9
    if failure == "partial":
        broker.read = lambda *_: iter(())
    if failure == "retention":
        broker.earliest = 1
    if failure == "foreign_group":
        broker.offsets[TOPICS[0] + ":0"] = 2
    before = dict(broker.offsets)
    with pytest.raises(ValueError):
        capture(tmp_path, broker, max_bytes=1 if failure == "bytes" else 100000)
    assert broker.offsets == before and not (tmp_path / "pending.json").exists()


@pytest.mark.parametrize("failure", ["raw", "manifest", "sink", "identity", "offset", "receipt"])
def test_recovery_rejects_corruption_and_foreign_progress(tmp_path, failure):
    broker, sink = Broker(), Sink()
    capture(tmp_path, broker)
    token, manifest = pending(tmp_path)
    receipt = land(tmp_path, sink)
    if failure == "raw":
        (tmp_path / "batches" / token / "raw.jsonl").write_text("corrupt")
    if failure == "manifest":
        write_json(tmp_path / "batches" / token / "manifest.json", manifest | {"source": "real"})
    if failure == "sink":
        sink.directories["batches/" + token]["events.jsonl"] = b"conflict"
    if failure == "identity":
        broker.id["topic_ids"][TOPICS[0]] = "recreated"
    if failure == "offset":
        broker.offsets[TOPICS[0] + ":0"] = 100
    if failure == "receipt":
        write_json(tmp_path / "landed.json", receipt | {"snapshot_id": "corrupt"})
    with pytest.raises(ValueError):
        land(tmp_path, sink) if failure == "sink" else acknowledge(tmp_path, broker)
    assert not (tmp_path / "state.json").exists()


def resolve(receipt):
    spec = importlib.util.spec_from_file_location("ods_input", Path(__file__).resolve().parents[1] / "warehouse/spark/ods_input.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    snapshot = {k: receipt[k] for k in ("schema_version", "source", "identity", "offsets", "batches", "root")}
    return module.resolve_snapshot(receipt["input"], canonical(snapshot), "events")


def test_incremental_snapshot_is_frozen_and_excludes_staging(tmp_path):
    broker, sink = Broker(), Sink()
    capture(tmp_path, broker, max_records=1)
    receipt = land(tmp_path, sink)
    paths, provenance = resolve(receipt)
    assert len(paths) == 1 and "/batches/" in paths[0] and paths[0].endswith("/events.jsonl")
    assert provenance["offsets"][TOPICS[0] + ":0"] == 1
    acknowledge(tmp_path, broker)
    capture(tmp_path, broker)
    second = land(tmp_path, sink)
    assert len(resolve(second)[0]) == 2 and resolve(receipt)[0] == paths
    assert digest(canonical({k: receipt[k] for k in ("schema_version", "source", "identity", "offsets", "batches", "root")})) == receipt["snapshot_id"]


def test_partial_partition_commit_and_crash_after_local_checkpoint(tmp_path):
    broker, sink = Broker(), Sink()
    capture(tmp_path, broker)
    receipt = land(tmp_path, sink)
    broker.offsets[TOPICS[0] + ":0"] = receipt["offsets"][TOPICS[0] + ":0"]
    write_json(tmp_path / "state.json", receipt)  # Crash between local commit and pending unlink.
    assert capture(tmp_path, broker) == receipt["batch_id"]
    assert land(tmp_path, sink) == receipt
    acknowledge(tmp_path, broker)
    assert not (tmp_path / "pending.json").exists()
    assert capture(tmp_path, broker) is None


def test_corrupt_checkpoint_cannot_be_extended_into_a_new_snapshot(tmp_path):
    broker, sink = Broker(), Sink()
    capture(tmp_path, broker)
    receipt = land(tmp_path, sink)
    acknowledge(tmp_path, broker)
    receipt["batches"][0]["counts"]["events"] = 1000
    write_json(tmp_path / "state.json", receipt)
    with pytest.raises(ValueError, match="checksum"):
        capture(tmp_path, broker)
