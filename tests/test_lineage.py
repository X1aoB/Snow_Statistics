import json
import sqlite3

import pytest

from snow_statistics.lineage import Capture, Journal, file_dataset, flush, input_dataset, validate_event
from snow_statistics.scheduling import resolve_window, retry_seconds

JOB = "snow_models.compute_operations"
INPUT = input_dataset("hdfs://192.168.216.131:9000/snow/ods/synthetic/kafka/test/snapshots/" + "a" * 64 + "/_snapshot.json")
OUTPUT = [dict(namespace="hive://snow-control:9083", name="snow_synthetic.ops_test_dim_content_scd2")]


def test_lifecycle_retry_identity_and_no_false_outputs(tmp_path):
    journal = Journal(tmp_path / "lineage.sqlite")
    first = journal.append(JOB, "test-t1", "START", INPUT)
    journal.append(JOB, "test-t1", "FAIL", INPUT)
    assert first != journal.append(JOB, "test-t2", "START", INPUT)
    journal.append(JOB, "test-t2", "COMPLETE", INPUT, OUTPUT)
    journal.append(JOB, "test-t2", "COMPLETE", INPUT, OUTPUT)
    assert journal.status("test") == dict(events=4, acknowledged=0, pending=4, open_runs=[])
    with pytest.raises(ValueError):
        journal.append(JOB, "test-t1", "COMPLETE", INPUT, OUTPUT)
    with pytest.raises(ValueError):
        journal.append(JOB, "test-t3", "FAIL", INPUT)
    with pytest.raises(ValueError):
        journal.append(JOB, "test-t2", "COMPLETE", INPUT, [file_dataset("other.json")])
    for _, event, _ in journal.pending("test"):
        validate_event(event)
        assert not event["outputs"] if event["eventType"] != "COMPLETE" else event["outputs"] == OUTPUT


def test_delivery_outage_ack_gap_and_checkpoint_import(tmp_path):
    journal = Journal(tmp_path / "lineage.sqlite")
    journal.append(JOB, "test-t1", "START", INPUT)
    journal.append(JOB, "test-t1", "COMPLETE", INPUT, OUTPUT)
    journal.backup(tmp_path / "copy.sqlite")
    assert Journal(tmp_path / "copy.sqlite").pending("test") == journal.pending("test")
    with pytest.raises(ValueError):
        journal.backup(journal.path)
    seen = []
    def down(event):
        raise OSError("receiver offline")
    with pytest.raises(OSError):
        flush(journal, "http://127.0.0.1:5000", "test", send=down)
    with pytest.raises(RuntimeError):
        flush(journal, "http://127.0.0.1:5000", "test", send=seen.append, fail_after_send=True)
    assert journal.status("test")["pending"] == 2
    flush(journal, "http://127.0.0.1:5000", "test", send=seen.append)
    assert seen[0] == seen[1] and seen[-1]["eventType"] == "COMPLETE"
    assert flush(journal, "http://127.0.0.1:5000", "test", send=seen.append) == 0
    assert journal.status("replacement-backend")["pending"] == 2
    receipt = journal.receipt("test")
    journal.acknowledge(receipt)
    receipt["acknowledgements"][0]["sha256"] = "b" * 64
    with pytest.raises(ValueError, match="match"):
        journal.acknowledge(receipt)


def test_capture_disabled_and_failure_isolation(tmp_path, monkeypatch, capsys):
    path = tmp_path / "lineage.sqlite"
    monkeypatch.setenv("SNOW_LINEAGE_DB", str(path))
    monkeypatch.delenv("SNOW_LINEAGE_ENABLED", raising=False)
    with Capture(JOB, "off", lambda: pytest.fail("off must not inspect inputs")):
        pass
    assert not path.exists()
    monkeypatch.setenv("SNOW_LINEAGE_ENABLED", "true")
    with Capture(JOB, "good", lambda: INPUT) as capture:
        capture.outputs(lambda: OUTPUT)
    with pytest.raises(RuntimeError, match="business exception"), Capture(JOB, "bad", lambda: INPUT):
        raise RuntimeError("business exception: private context")
    events = Journal(path).pending("test")
    assert [e[1]["eventType"] for e in events] == ["START", "COMPLETE", "START", "FAIL"]
    assert "private context" not in json.dumps(events)
    monkeypatch.setattr(Journal, "append", lambda *a, **kw: (_ for _ in ()).throw(sqlite3.OperationalError("disk full")))
    with Capture(JOB, "disk-full", lambda: INPUT):
        value = 123
    assert value == 123 and "capture_failed" in capsys.readouterr().err


def test_interrupted_run_needs_authoritative_failure_state(tmp_path):
    journal = Journal(tmp_path / "lineage.sqlite")
    key = resolve_window("2026-01-05T00:00:00Z", "failure-demo")["run_id"] + "-t1"
    journal.append(JOB, key, "START", INPUT)
    receipt = dict(dag_id="snow_models", runs=[dict(run_id="failure-demo")], tasks=[dict(task_id="compute_operations", try_number=1, state="running", end_date=None)])
    assert journal.reconcile_failed(receipt) == 0
    assert len(journal.status("test")["open_runs"]) == 1
    receipt["tasks"][0].update(state="failed", end_date="2026-09-11 10:00:00")
    assert journal.reconcile_failed(receipt) == 1
    assert journal.reconcile_failed(receipt) == 0
    assert not journal.status("test")["open_runs"]


def test_schema_checksum_capacity_and_endpoint_guards(tmp_path):
    journal = Journal(tmp_path / "lineage.sqlite", max_bytes=32768)
    journal.append(JOB, "valid", "START", INPUT)
    with pytest.raises(ValueError):
        journal.append(JOB, "invalid", "START", INPUT, OUTPUT)
    with pytest.raises(ValueError):
        input_dataset("hdfs://snow-control:9000/snow/ods/synthetic/events?key=private")
    with pytest.raises(ValueError):
        file_dataset("../business.db")
    with pytest.raises(ValueError):
        flush(journal, "http://example.org", "test")
    with journal.connection() as db:
        db.execute("UPDATE events SET sha256='wrong'")
    with pytest.raises(ValueError, match="checksum"):
        flush(journal, "http://127.0.0.1:5000", "test", send=lambda e: pytest.fail("corruption must not be sent"))
    with pytest.raises(sqlite3.DatabaseError):
        for n in range(200):
            journal.append(JOB, "capacity-" + str(n), "START", INPUT)
    assert journal.path.stat().st_size <= 32768
    assert retry_seconds("15") == 15
    with pytest.raises(ValueError):
        retry_seconds("1")
