import json

import pytest

from snow_statistics.lineage import Journal
from snow_statistics.real_lineage import RealCapture, RealJournal, dataset, flush_real, validate_event


def inputs():
    return [dataset("hdfs://snow-control:9000/snow/ods/real/kafka/test/snapshots/" + "a" * 64 + "/_snapshot.json")]


def test_actual_wrapper_transitions_metadata_only_and_failed_outputs_excluded(tmp_path):
    journal = RealJournal(tmp_path / "real.sqlite")
    output = dataset("hdfs://snow-control:9000/snow/warehouse/real/test/iceberg/run/analytics/daily")
    with RealCapture(journal, "snow_real.iceberg_aggregates", "first", inputs()) as capture:
        capture.outputs = [output]
    assert journal.status("test")["events"] == 2
    events = [event for _, event, _ in journal.pending("test")]
    assert [event["eventType"] for event in events] == ["START", "COMPLETE"]
    assert events[1]["job"]["namespace"] == "snow-statistics.real"
    assert all(set(item) == {"namespace", "name"} for event in events for item in event["inputs"] + event["outputs"])
    assert "anonymous_id" not in json.dumps(events)
    with pytest.raises(RuntimeError):
        with RealCapture(journal, "snow_real.iceberg_aggregates", "failed", inputs()) as capture:
            capture.outputs = [output]
            raise RuntimeError("synthetic injected failure")
    assert journal.pending("test")[-1][1]["eventType"] == "FAIL"
    assert journal.pending("test")[-1][1]["outputs"] == []


def test_lineage_delivery_ack_is_after_actual_send_and_replay_is_idempotent(tmp_path):
    journal = RealJournal(tmp_path / "real.sqlite")
    journal.append("snow_real.compute_daily", "run", "START", inputs())
    def unavailable(_):
        raise OSError("fixture transport failure")
    with pytest.raises(OSError):
        flush_real(journal, "http://127.0.0.1:5000", "test", send=unavailable)
    assert journal.status("test")["pending"] == 1
    sent = []
    assert flush_real(journal, "http://127.0.0.1:5000", "test", send=sent.append) == 1
    assert flush_real(journal, "http://127.0.0.1:5000", "test", send=sent.append) == 0
    assert len(sent) == 1


def test_no_synthetic_namespace_private_facets_or_fabricated_hive_claim(tmp_path):
    journal = RealJournal(tmp_path / "real.sqlite")
    journal.append("snow_real.compute_daily", "run", "START", inputs())
    event = journal.pending("test")[0][1]
    event["run"]["facets"] = {"raw": "private"}
    with pytest.raises(ValueError):
        validate_event(event)
    for uri in ("hdfs://snow-control:9000/snow/ods/synthetic/input.json", "hive://snow-control:9083/snow_real.unverified", "file://snow-control/etc/passwd"):
        with pytest.raises(ValueError):
            dataset(uri)
    mixed = Journal(tmp_path / "synthetic.sqlite")
    mixed.append("snow_models.publish", "old", "START", [])
    with pytest.raises(ValueError, match="reuse"):
        RealJournal(mixed.path).append("snow_real.publish", "new", "START", [])
