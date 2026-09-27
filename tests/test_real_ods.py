import copy
import importlib.util
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from test_landing import Broker, Sink, record

from snow_statistics.landing import acknowledge, capture, land, normalize, topics_for
from snow_statistics.publication import canonical
from snow_statistics.real_ods import expire_window
from snow_statistics.simulator import generate

AT = datetime(2026, 1, 1, 1, tzinfo=UTC)
COLLECTOR = dict(schema_version=1, source="real", instance_id="fbf85904-0a98-44f4-bd13-804280831449",
                 generation="c2b34a4a-a18d-47ac-bfe5-acb41444e3d4")


class RealBroker(Broker):
    def __init__(self):
        super().__init__()
        topic = topics_for("real")[0]
        self.id["topic_ids"] = {topic: "incarnation"}
        self.id["collector"] = COLLECTOR.copy()
        self.rows = {topic + ":0": [record(r | {"source": "real"}, i, topic)
                                   for i, r in enumerate(generate(users=1)["events"][:3])]}


class RealSink(Sink):
    root = "hdfs://snow-control:9000/snow/ods/real/kafka/test"
    def delete_real_directory(self, relative):
        self.directories.pop(relative, None)


def resolve(receipt, now):
    spec = importlib.util.spec_from_file_location("ods_input", Path(__file__).resolve().parents[1] / "warehouse/spark/ods_input.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    body = {k: receipt[k] for k in ("schema_version", "source", "identity", "offsets", "batches", "root", "head_batch_id")}
    return module.resolve_snapshot(receipt["input"], canonical(body), "events", "real", now)


def test_real_topics_exclude_cdc_and_cross_source_archive(tmp_path):
    assert topics_for("real") == ("snow.real.events.v1",)
    with pytest.raises(ValueError, match="provenance"):
        capture(tmp_path, Broker(), provenance="real", now=AT)
    with pytest.raises(ValueError, match="envelope"):
        normalize(record(generate(users=1)["events"][0], 0, "snow.real.events.v1"), "real")


def test_real_expiry_preserves_offset_head_and_removes_old_manifest_references(tmp_path):
    broker, sink = RealBroker(), RealSink()
    capture(tmp_path, broker, provenance="real", now=AT)
    receipt = land(tmp_path, sink, now=AT)
    acknowledge(tmp_path, broker)
    assert resolve(receipt, AT)[1]["source"] == "real"
    assert resolve(receipt, AT)[1]["collector"] == COLLECTOR
    before = copy.deepcopy(broker.offsets)
    with pytest.raises(ValueError, match="expired"):
        resolve(receipt, AT + timedelta(days=7))
    result = expire_window(tmp_path, sink, now=AT + timedelta(days=7))
    assert result["removed"] == 1 and result["window_batches"] == 0
    assert result["offsets"] == before == broker.offsets
    assert all(not k.startswith("batches/") for k in sink.directories)
    assert len(sink.directories) == 1  # one current empty data-window manifest
    assert capture(tmp_path, broker, provenance="real", now=AT + timedelta(days=7)) is None


def test_failed_backend_expiry_blocks_capture_then_resumes(tmp_path):
    broker, sink = RealBroker(), RealSink()
    capture(tmp_path, broker, provenance="real", now=AT)
    land(tmp_path, sink, now=AT)
    acknowledge(tmp_path, broker)
    original = sink.delete_real_directory
    sink.delete_real_directory = lambda _: (_ for _ in ()).throw(RuntimeError("HDFS unavailable"))
    with pytest.raises(RuntimeError):
        expire_window(tmp_path, sink, now=AT + timedelta(days=7))
    with pytest.raises(ValueError, match="cleanup"):
        capture(tmp_path, broker, provenance="real", now=AT + timedelta(days=7))
    sink.delete_real_directory = original
    assert expire_window(tmp_path, sink, now=AT + timedelta(days=7))["removed"] == 1


def test_old_real_input_is_not_archived(tmp_path):
    with pytest.raises(ValueError, match="expired"):
        capture(tmp_path, RealBroker(), provenance="real", now=AT + timedelta(days=7))
    assert not (tmp_path / "batches").exists()


def test_real_capture_requires_bound_collector_even_when_recovering_pending(tmp_path):
    broker = RealBroker()
    broker.id.pop("collector")
    with pytest.raises(ValueError, match="collector"):
        capture(tmp_path, broker, provenance="real", now=AT)
    assert not (tmp_path / "batches").exists()
    broker.id["collector"] = COLLECTOR.copy()
    capture(tmp_path, broker, provenance="real", now=AT)
    broker.id["collector"]["generation"] = "59d28bda-f5c8-4ec0-9ea4-031f3a580f34"
    with pytest.raises(ValueError, match="another source identity"):
        capture(tmp_path, broker, provenance="real", now=AT)
    land(tmp_path, RealSink(), now=AT)
    with pytest.raises(ValueError, match="identity"):
        acknowledge(tmp_path, broker)
    assert broker.offsets == {}


def test_recreated_collector_cannot_extend_old_ods_or_hide_in_spark_metadata(tmp_path):
    broker, sink = RealBroker(), RealSink()
    capture(tmp_path, broker, provenance="real", now=AT)
    receipt = land(tmp_path, sink, now=AT)
    acknowledge(tmp_path, broker)
    broker.id["collector"]["instance_id"] = "59d28bda-f5c8-4ec0-9ea4-031f3a580f34"
    with pytest.raises(ValueError, match="changed"):
        capture(tmp_path, broker, provenance="real", now=AT)
    receipt["identity"].pop("collector")
    snapshot = {k: receipt[k] for k in ("schema_version", "source", "identity", "offsets", "batches", "root", "head_batch_id")}
    from snow_statistics.io import digest
    token = digest(canonical(snapshot))
    receipt.update(snapshot_id=token, input=receipt["root"] + "/snapshots/" + token + "/_snapshot.json")
    with pytest.raises(ValueError, match="collector"):
        resolve(receipt, AT)
