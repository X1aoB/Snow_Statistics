import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from snow_statistics.io import digest
from snow_statistics.publication import canonical
from snow_statistics.real_backend_lifecycle import (
    CheckpointFiles,
    CheckpointRetention,
    DorisRetention,
    KafkaClient,
    KafkaRetention,
)

NOW = datetime.now(UTC)
TOPIC = "snow.real.fixture.events.v1"
TABLE = "snow_real_fixture.events_realtime"
CP_ROOT = "hdfs://snow-control:9000/snow/checkpoints/real/fixture"
CP = CP_ROOT + "/old-job"


def entry(kind="raw", age=8):
    days = 90 if kind == "aggregate" else 7
    origin = NOW - timedelta(days=age)
    return dict(kind=kind, original_min_accepted_at=origin.isoformat(),
                expires_at=(origin + timedelta(days=days)).isoformat())


class Broker:
    def __init__(self, ages=(8, 1)):
        self.rows = [(i, json.dumps(dict(source="real", accepted_at=(NOW - timedelta(days=age)).isoformat(),
                                       event={"private_fixture": "not emitted to evidence"})).encode()) for i, age in enumerate(ages)]
        self.start, self.end = 0, len(self.rows)
        self.deleted, self.fail_readback, self.bad_config = [], False, False

    def identity(self, topics):
        return dict(cluster_id="fixture-cluster", topic_ids={TOPIC: "fixture-id"})

    def bounds(self, topics):
        return {(TOPIC, 0): (self.start, self.end)}

    def records(self, key, start, end):
        return [(i, value) for i, value in self.rows if start <= i < end]

    def delete_prefixes(self, values):
        self.deleted.append(values)
        if not self.fail_readback:
            self.start = values[(TOPIC, 0)]

    def retention_settings(self, topics):
        return {"topics": {TOPIC: {"retention.ms": "-1" if self.bad_config else "604800000",
                                    "segment.ms": "60000", "file.delete.delay.ms": "60000",
                                    "cleanup.policy": "delete", "message.timestamp.type": "CreateTime"}},
                "brokers": {"1": {"log.retention.check.interval.ms": "300000"}}}


def kafka(broker):
    return KafkaRetention(broker, {TOPIC: "raw"}, "fixture-cluster", {TOPIC: "fixture-id"})


def test_kafka_original_time_deletion_readback_and_idempotence():
    broker = Broker()
    resources = {TOPIC: entry()}
    result = kafka(broker).purge_and_verify(resources, NOW)
    assert broker.deleted == [{(TOPIC, 0): 1}]
    assert result["live_records"] == 1 and result["remaining_expired"] == 0
    assert result["next_expiry"] == (NOW + timedelta(days=6)).isoformat()
    assert result["resources_sha256"] == digest(canonical(resources))
    assert "private_fixture" not in json.dumps(result)
    assert result["evidence"]["physical_segment_erasure_verified"] is False
    kafka(broker).purge_and_verify(resources, NOW)
    assert len(broker.deleted) == 1


@pytest.mark.parametrize("failure", ["mixed", "missing_origin", "retention", "ownership", "scope", "extended_expiry"])
def test_kafka_unsafe_state_blocks_before_any_delete(failure):
    broker = Broker((1, 8) if failure == "mixed" else (8, 1))
    resources = {TOPIC: entry()}
    if failure == "missing_origin":
        broker.rows[0] = (0, b'{"source":"real"}')
    if failure == "retention":
        broker.bad_config = True
    if failure == "ownership":
        broker.identity = lambda _: dict(cluster_id="other", topic_ids={TOPIC: "fixture-id"})
    if failure == "scope":
        resources["snow.synthetic.fixture.events.v1"] = entry()
    if failure == "extended_expiry":
        resources[TOPIC]["expires_at"] = (NOW + timedelta(days=10)).isoformat()
    with pytest.raises(ValueError):
        kafka(broker).purge_and_verify(resources, NOW)
    assert broker.deleted == []


def test_kafka_failed_delete_never_issues_receipt():
    broker = Broker()
    broker.fail_readback = True
    with pytest.raises(RuntimeError, match="readback"):
        kafka(broker).purge_and_verify({TOPIC: entry()}, NOW)


def test_kafka_native_config_decodes_pinned_protocol_field_names():
    # No Kafka installation is required by the lightweight CI environment.
    data = {"resources": [dict(error_code=0, resource_type=2, resource_name=TOPIC,
                               config_entries=[{"config_names": "retention.ms", "config_value": "604800000"}])]}
    result = KafkaClient.decode_settings([SimpleNamespace(to_object=lambda: data)])
    assert result["topics"][TOPIC]["retention.ms"] == "604800000"


class DorisConnection:
    def __init__(self, ages=(8, 1)):
        self.rows = [{"source": "real", "accepted_at": (NOW - timedelta(days=age)).replace(tzinfo=None)} for age in ages]
        self.deletes, self.fail_delete = [], False

    def cursor(self):
        parent = self
        class Cursor:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

            def execute(self, sql, values):
                if "information_schema.TABLES" in sql:
                    self.result = [("BASE TABLE",)]
                elif "source IS NULL" in sql:
                    self.result = [(sum(row["source"] != "real" or row["accepted_at"] is None for row in parent.rows),)]
                elif sql.startswith("SELECT MIN("):
                    at = [row["accepted_at"] for row in parent.rows]
                    self.result = [(min(at) if at else None, max(at) if at else None)]
                elif sql.startswith("DELETE"):
                    parent.deletes.append((sql, values))
                    if not parent.fail_delete:
                        parent.rows = [row for row in parent.rows if row["accepted_at"] > values[1]]
                    self.result = []
                elif sql.startswith("SELECT COUNT(*)"):
                    self.result = [(sum(row["accepted_at"] <= values[0] for row in parent.rows),)]
                elif sql.startswith("SELECT SUM("):
                    at = [row["accepted_at"] for row in parent.rows]
                    self.result = [(sum(at <= values[0] for at in at), len(at), min(at) if at else None)]
                else:
                    pytest.fail("Unexpected SQL: " + sql)

            def fetchall(self):
                return self.result
        return Cursor()


def test_doris_uses_accepted_at_scoped_sql_then_reads_back():
    connection = DorisConnection()
    result = DorisRetention(connection, {TABLE: "raw"}).purge_and_verify({TABLE: entry()}, NOW)
    assert result["live_records"] == 1
    assert len(connection.deletes) == 1
    sql, values = connection.deletes[0]
    assert sql == f"DELETE FROM {TABLE} WHERE source=%s AND accepted_at<=%s"
    assert values[0] == "real" and values[1] == (NOW - timedelta(days=7)).replace(tzinfo=None)
    assert result["evidence"][TABLE]["physical_compaction_erasure_verified"] is False


@pytest.mark.parametrize("failure", ["mixed_source", "predates_registry", "delete_readback", "unknown_table"])
def test_doris_misregistered_or_unpurged_table_never_gets_permit(failure):
    connection = DorisConnection()
    resources, allow = {TABLE: entry()}, {TABLE: "raw"}
    if failure == "mixed_source":
        connection.rows[0]["source"] = "synthetic"
    if failure == "predates_registry":
        resources[TABLE] = entry(age=6)
    if failure == "delete_readback":
        connection.fail_delete = True
    if failure == "unknown_table":
        resources = {"snow_realtime_fixture.events_realtime": entry()}
    with pytest.raises((ValueError, RuntimeError)):
        DorisRetention(connection, allow).purge_and_verify(resources, NOW)
    if failure != "delete_readback":
        assert not connection.deletes


class Hdfs:
    def __init__(self):
        self.nodes = {CP_ROOT: [(CP, "DIRECTORY")], CP: [(CP + "/state", "FILE")]}
        self.deleted = []

    def path(self, value):
        assert value.startswith(CP_ROOT + "/")

    def children(self, value):
        return self.nodes.get(value, [])

    def exists(self, value):
        return value in self.nodes

    def delete_exact(self, value):
        self.deleted.append(value)
        self.nodes.pop(value, None)
        self.nodes[CP_ROOT] = []


class Flink:
    running = False

    def require_quiescent(self):
        if self.running:
            raise ValueError("Real Flink job is running")


def test_checkpoint_expiry_is_original_time_and_stopped_jobs_only():
    hdfs, flink = Hdfs(), Flink()
    files = CheckpointFiles([CP_ROOT], hdfs)
    adapter = CheckpointRetention(files, flink, {CP: "raw"})
    flink.running = True
    with pytest.raises(ValueError):
        adapter.purge_and_verify({CP: entry()}, NOW)
    assert hdfs.deleted == []
    flink.running = False
    result = adapter.purge_and_verify({CP: entry()}, NOW)
    assert hdfs.deleted == [CP] and result["live_records"] == 0 and result["next_expiry"] is None
    adapter.purge_and_verify({CP: entry()}, NOW)
    assert hdfs.deleted == [CP]


def test_checkpoint_unknown_payload_or_escape_blocks_without_delete():
    hdfs = Hdfs()
    hdfs.nodes[CP_ROOT].append((CP_ROOT + "/unregistered-state", "FILE"))
    files = CheckpointFiles([CP_ROOT], hdfs)
    with pytest.raises(ValueError, match="Unregistered"):
        CheckpointRetention(files, Flink(), {CP: "raw"}).purge_and_verify({CP: entry()}, NOW)
    with pytest.raises(ValueError, match="escaped"):
        files.delete_exact(CP_ROOT + "/../synthetic")
    assert hdfs.deleted == []
