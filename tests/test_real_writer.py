"""Synthetic metadata exercises for the actual production adapter boundary."""
import base64
import json
import os
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid5

import pytest
from test_real_writer_bootstrap import BootstrapDocker

from snow_statistics import real_writer as module
from snow_statistics.real_quiescent import TABLES, topic_names, validate_initial

COLLECTOR = dict(schema_version=1, source="real", instance_id="00000000-0000-4000-8000-000000000001",
                 generation="00000000-0000-4000-8000-000000000002")
MANIFEST = dict(event_lane="real_fixture", epoch_id="real-fixture", generation="00000000-0000-4000-8000-000000000003",
                original_min_accepted_at="2026-09-13T00:00:00+00:00", expires_at="2026-09-20T00:00:00+00:00",
                containers=dict(kafka="snow-real-real-fixture-kafka", jobmanager="snow-real-real-fixture-jobmanager"))


def test_collector_only_uses_loopback_and_reader_file(tmp_path, monkeypatch):
    calls = []
    token = tmp_path / "reader.token"
    token.write_text("a" * 43)
    os.chmod(token, 0o600)
    class Client:
        def __init__(self, **kwargs):
            assert kwargs["trust_env"] is False and kwargs["follow_redirects"] is False
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def get(self, path, **kwargs):
            calls.append((path, kwargs))
            value = COLLECTOR | dict(latest_accepted_seq=0, earliest_available_seq=None, expired_through=0, aggregate_cursor=0)
            return SimpleNamespace(status_code=200, content=b"{}", json=lambda: value)
    import httpx
    monkeypatch.setattr(httpx, "Client", Client)
    assert module.collector_status("http://127.0.0.1:18100", token) == COLLECTOR
    assert calls[0][0] == "/analytics/private/v1/status"
    assert calls[0][1]["headers"]["Authorization"] == "Bearer " + "a" * 43
    for url in ("https://example.com", "http://user@localhost", "http://localhost?token=x", "http://localhost/events"):
        with pytest.raises(ValueError):
            module.collector_status(url, token)
    assert len(calls) == 1


def test_sql_and_environment_cannot_escape_epoch():
    for name in ("snow", "snow_real_x;DROP DATABASE snow", "other_real_fixture"):
        with pytest.raises(ValueError):
            module.sql_statements("CREATE DATABASE IF NOT EXISTS snow;", name)
    schema = (Path(__file__).resolve().parents[1] / "warehouse/doris/schema.sql").read_text()
    rendered = module.sql_statements(schema, "snow_real_real_fixture")
    assert all("snow." not in statement for statement in rendered)
    names = topic_names(MANIFEST)
    identity = dict(cluster_id="cluster_fixture_001", topic_ids={name: "topic_fixture_0001" for name in names})
    registration = dict(initial=dict(kafka=dict(identity=identity, bounds={names[0]: dict(end=0)})))
    environment = module.job_environment(MANIFEST, registration, dict(user="sr_real_fixture", password="a" * 43), "192.168.216.133")
    assert not any(key.startswith("SNOW_REAL_EPOCH_") for key in environment)
    assert environment["SNOW_REAL_READABLE_FROM"] == MANIFEST["original_min_accepted_at"]
    assert environment["DORIS_TABLE"] == "snow_real_real_fixture.events_realtime"


def test_probe_reads_engine_values_and_rejects_hidden_partitions(tmp_path, monkeypatch):
    names = set(topic_names(MANIFEST))
    partitions = {name: 0 for name in names}
    docker = BootstrapDocker(MANIFEST["containers"]["jobmanager"])
    class Broker:
        def __init__(self, *args):
            pass
        def identity(self, topics):
            assert topics == names
            return dict(cluster_id=base64.urlsafe_b64encode(UUID(MANIFEST["generation"]).bytes).decode().rstrip("="),
                        topic_ids={name: base64.urlsafe_b64encode(uuid5(UUID(MANIFEST["generation"]), name).bytes).decode().rstrip("=") for name in names})
        def bounds(self, topics):
            return {(name, partitions[name]): (0, 0) for name in topics}
        def close(self):
            pass
    monkeypatch.setattr(module, "KafkaClient", Broker)
    monkeypatch.setattr(module, "frozen", lambda epoch: MANIFEST)
    writer = object.__new__(module.ActualWriter)
    writer.epoch = SimpleNamespace(docker=docker)
    writer.host, writer.database = "192.168.216.133", "snow_real_real_fixture"
    writer.identity = lambda: COLLECTOR
    writer.check_namespaces = lambda topics, databases: None
    writer.flink = lambda path: {"jobs": []}
    writer.read_tables = lambda: {name: "BASE TABLE" for name in TABLES}
    writer.sql = lambda query: [(name, "BASE TABLE") for name in TABLES] + [("daily_realtime", "VIEW")] if query.startswith("SHOW") else [(0,)]
    value = writer.read_initial_state(MANIFEST, COLLECTOR)
    assert validate_initial(value, MANIFEST) == value
    docker.add("/checkpoints/unregistered-payload")
    with pytest.raises(ValueError):
        validate_initial(writer.read_initial_state(MANIFEST, COLLECTOR), MANIFEST)
    partitions[next(iter(names))] = 1
    with pytest.raises(ValueError):
        writer.read_initial_state(MANIFEST, COLLECTOR)


def test_job_readback_cannot_adopt_a_supplied_job_or_leak_failure():
    writer = object.__new__(module.ActualWriter)
    writer.submitted = None
    with pytest.raises(ValueError):
        writer.read_job("a" * 32)
    report = module.failure_metadata(ValueError("password=private-fixture-value"))
    assert "private-fixture-value" not in json.dumps(report)


def test_every_doris_read_and_mutation_stops_when_epoch_gate_expires():
    calls = []
    cursor = SimpleNamespace(execute=lambda *a: calls.append("execute"),
                             executemany=lambda *a: calls.append("executemany"), fetchall=lambda: calls.append("fetchall"))
    allowed = True
    def guard():
        if not allowed:
            raise ValueError("expired fixture gate")
    wrapped = module.GuardedCursor(cursor, guard)
    wrapped.execute("SELECT 1")
    allowed = False
    for operation in (lambda: wrapped.execute("INSERT"), lambda: wrapped.executemany("INSERT", []), wrapped.fetchall):
        with pytest.raises(ValueError):
            operation()
    assert calls == ["execute"]


def test_initialization_rejects_other_lanes_and_unregistered_namespaces():
    module.validate_namespaces(["__consumer_offsets"], ["information_schema", "mysql", "__internal_schema"])
    topics = topic_names(MANIFEST)
    module.validate_namespaces(topics, ["information_schema", "snow_real_real_fixture"], topics, ["snow_real_real_fixture"])
    for other_topics, databases in ((["snow.synthetic.events.v1"], []), ([], ["snow_old"]),
                                   (["__unrecognized"], []), (topics[:-1], ["snow_real_real_fixture"])):
        with pytest.raises(ValueError):
            module.validate_namespaces(other_topics, databases, topics, ["snow_real_real_fixture"])


def test_account_sql_formats_percent_host_without_driver_format_conflict():
    statements = module.account_statements("snow_real_real_fixture", dict(user="sr_real_fixture", password="fixture"))
    rendered = [sql % tuple(repr(value) for value in parameters) for sql, parameters in statements]
    assert rendered[0] == "CREATE ROLE `sr_real_fixture_role`"
    assert "@'%' IDENTIFIED BY 'fixture' DEFAULT ROLE 'sr_real_fixture_role'" in rendered[2]
    with pytest.raises(ValueError, match="outside"):
        module.account_statements("snow_real_real_fixture", dict(user="other_user", password="fixture"))


def test_locked_doris_full_table_shape_rejects_schema_drift_and_extra_views():
    database = "snow_real_real_fixture"
    columns = ["Tables_in_" + database, "Table_type", "Storage_format", "Inverted_index_storage_format"]
    rows = [(name, "BASE TABLE", "V2", "V2") for name in TABLES] + [
        (name, "VIEW", "NONE", "NONE") for name in ("daily_realtime", "daily_published", "report_published")]
    calls = []
    cursor = SimpleNamespace(execute=calls.append, description=[(name,) for name in columns], fetchall=lambda: rows)
    assert module.read_owned_tables(cursor, database)["events_realtime"] == "BASE TABLE"
    assert calls == ["SHOW FULL TABLES FROM " + database]
    cursor.description = [(name,) for name in columns[:2]]
    with pytest.raises(ValueError, match="metadata columns"):
        module.read_owned_tables(cursor, database)
    cursor.description = [(name,) for name in columns]
    for bad in (rows + [("extra", "VIEW", "NONE", "NONE")], [row[:2] for row in rows],
                [*rows[:-1], rows[0]], [(rows[0][0], "BASE TABLE", "V1", "V2"), *rows[1:]]):
        cursor.fetchall = lambda: bad
        with pytest.raises(ValueError):
            module.read_owned_tables(cursor, database)
