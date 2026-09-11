import json
from datetime import UTC, datetime

import pytest

from snow_statistics.io import digest, write_json
from snow_statistics.log_adapter import from_log
from snow_statistics.model import build, classify, deduplicate
from snow_statistics.simulator import generate
from snow_statistics.sync import sync_once


def test_seed_clock_scd2_ticket_reopen_and_funnel():
    fixture = generate(users=2)
    assert fixture == generate(users=2)
    assert fixture != generate(seed=43, users=2)
    result = build(fixture)
    assert result["quality"] == dict(raw=28, valid=28, duplicates=0, quarantined=0)
    versions = result["content_scd2"]
    assert classify(versions, "content-1", "2026-01-01T12:00:00.000Z")["category"] == "data"
    assert classify(versions, "content-1")["category"] == "engineering"
    assert classify(versions, "content-2") is None
    assert len(result["ticket_rounds"]) == 4
    assert [r["duration_seconds"] for r in result["ticket_rounds"]] == [3600, 1800, 3600, 1800]
    assert len(result["conversions"]) == 1  # user 0 exceeds 30 minutes, user 1 converts.
    assert result["ticket_rounds"][0]["first_resolved_at"] != result["ticket_rounds"][0]["latest_resolved_at"]


def test_operations_as_of_extends_daily_snapshots_and_excludes_future_changes():
    fixture = generate(users=2)
    january_fourth = build(fixture, operations_as_of="2026-01-04")
    assert len(january_fourth["ticket_daily"]) == 8
    assert {r["date"] for r in january_fourth["ticket_daily"]} == {"2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04"}
    january_first = build(fixture, operations_as_of="2026-01-01")
    assert len(january_first["ticket_daily"]) == 2
    assert classify(january_first["content_scd2"], "content-1")["category"] == "data"
    assert classify(january_first["content_scd2"], "content-2") is not None
    assert january_fourth["daily"] == january_first["daily"]  # Independent behavior metric window.
    assert build(fixture, operations_as_of="2025-12-31")["ticket_daily"] == []


def test_replay_and_bad_rows_reconcile():
    fixture = generate(users=2)
    bad = {"seq": 999, "source": "synthetic", "event": {"body": "not retained"}}
    valid, quality, quarantine = deduplicate(fixture["events"] * 2 + [bad])
    assert quality == dict(raw=57, valid=28, duplicates=28, quarantined=1)
    assert "not retained" not in json.dumps(quarantine)
    assert len(valid) == 28


def test_sync_archive_replay_after_partial_publish(tmp_path):
    rows = generate(users=1)["events"][:2]
    response = {"schema_version": 1, "events": rows, "next_cursor": 2}
    calls = []
    def fail(row):
        calls.append(row["seq"])
        if row["seq"] == 2:
            raise RuntimeError("sink interrupted")
    with pytest.raises(RuntimeError):
        sync_once(tmp_path, lambda after: response, fail)
    assert not (tmp_path / "cursor.json").exists()
    assert sync_once(tmp_path, lambda _: pytest.fail("must replay archive"), lambda row: calls.append(row["seq"])) == 2
    assert calls == [1, 2, 1, 2]
    assert json.loads((tmp_path / "cursor.json").read_text())["cursor"] == 2


def test_sync_tamper_and_gap(tmp_path):
    rows = generate(users=1)["events"][:2]
    with pytest.raises(ValueError):
        sync_once(tmp_path, lambda _: {"events": rows[1:], "next_cursor": 2}, lambda _: None)
    with pytest.raises(RuntimeError):
        sync_once(tmp_path, lambda _: {"events": rows, "next_cursor": 2}, lambda _: (_ for _ in ()).throw(RuntimeError()))
    pending = json.loads((tmp_path / "pending.json").read_text())
    (tmp_path / pending["archive"]).write_text("corrupt")
    with pytest.raises(ValueError):
        sync_once(tmp_path, lambda _: None, lambda _: None)


def test_sync_crash_after_cursor_commit(tmp_path):
    response = {"events": generate(users=1)["events"][:2], "next_cursor": 2}
    write_json(tmp_path / "batches/fixture.json", response)
    write_json(tmp_path / "pending.json", {"archive": "batches/fixture.json", "after": 0,
               "sha256": digest((tmp_path / "batches/fixture.json").read_bytes())})
    write_json(tmp_path / "cursor.json", {"cursor": 2})
    assert sync_once(tmp_path, lambda _: None, lambda _: pytest.fail("already committed")) == 0


def test_log_adapter_allowlist_and_idempotence():
    log = dict(event="public_generation_complete", request_id="r1", character_id="sample_character",
               stage="complete", elapsed_ms=100, terminal_error="", exception_type=None, body="secret", ip="private")
    stamp = datetime(2026, 1, 1, tzinfo=UTC).isoformat()
    event = from_log(log, stamp)
    assert event == from_log(log, stamp)
    assert event["success"] is True
    assert "secret" not in json.dumps(event) and "private" not in json.dumps(event)
    assert from_log(log | {"terminal_error": "some private diagnostic"}, stamp)["success"] is False
