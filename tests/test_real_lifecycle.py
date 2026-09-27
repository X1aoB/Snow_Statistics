import json
from datetime import UTC, datetime, timedelta

import pytest

from snow_statistics.lifecycle import RealLifecycle
from snow_statistics.simulator import generate
from snow_statistics.source_cursor import SourceGap, check_source, rebase
from snow_statistics.sync import sync_once

AT = datetime(2026, 1, 1, tzinfo=UTC)


def status(**changes):
    return dict(schema_version=1, source="real", instance_id="00000000-0000-0000-0000-000000000001",
                generation="00000000-0000-0000-0000-000000000002", earliest_available_seq=1,
                latest_accepted_seq=2, expired_through=0, aggregate_cursor=2) | changes


def test_copy_restore_cannot_extend_expiry_and_cleanup_precedes_read(tmp_path):
    manager = RealLifecycle(tmp_path / "real")
    manager.initialize()
    raw = manager.register("batches/a.json", "raw", AT.isoformat(), now=AT)
    raw.parent.mkdir(parents=True)
    raw.write_text("synthetic test bytes")
    copy = manager.register("backups/a.json", "raw", AT.isoformat(), copied_from="batches/a.json", now=AT + timedelta(days=6))
    copy.parent.mkdir(parents=True)
    copy.write_bytes(raw.read_bytes())
    with pytest.raises(ValueError, match="reset"):
        manager.register("backups/new.json", "raw", (AT + timedelta(days=6)).isoformat(), copied_from="batches/a.json", now=AT + timedelta(days=6))
    with pytest.raises(ValueError, match="Cleanup"):
        manager.readable("batches/a.json", AT)
    manager.cleanup(AT)
    assert manager.readable("batches/a.json", AT).read_bytes() == copy.read_bytes()
    with pytest.raises(ValueError, match="Cleanup"):
        manager.readable("batches/a.json", AT + timedelta(days=7))
    result = manager.cleanup(AT + timedelta(days=9))  # Machine was switched off on day 7.
    assert result["removed"] == 2 and not raw.exists() and not copy.exists()


def test_cleanup_invalidates_referencing_snapshots_not_aggregate_or_synthetic(tmp_path):
    manager = RealLifecycle(tmp_path / "real")
    manager.initialize()
    for name, kind, refs in (("raw", "raw", ()), ("snapshot", "auxiliary", ("raw",)), ("daily", "aggregate", ())):
        manager.register(name, kind, AT.isoformat(), references=refs, now=AT).write_text("fixture")
    synthetic = tmp_path / "synthetic"
    synthetic.write_text("retained")
    assert manager.plan(AT + timedelta(days=7)) == ["raw", "snapshot"]
    manager.cleanup(AT + timedelta(days=7))
    assert synthetic.read_text() == "retained" and manager.readable("daily", AT + timedelta(days=7)).exists()


def test_unregistered_or_unsafe_paths_fail_closed(tmp_path):
    manager = RealLifecycle(tmp_path / "real")
    manager.initialize()
    for path in ("../synthetic", "C:/business", "registry.json", "nested/../../outside", "nested\\other"):
        with pytest.raises(ValueError):
            manager.register(path, "raw", AT.isoformat(), now=AT)
    (manager.root / "unregistered.json").write_text("fixture")
    with pytest.raises(ValueError, match="Unregistered"):
        manager.cleanup(AT)
    assert json.loads((manager.root / "gate.json").read_bytes())["open"] is False


def test_source_rebuild_gap_and_rebase_are_explicit(tmp_path):
    old = tmp_path / "old"
    check_source(old, status(), 0, expected_source="real")
    with pytest.raises(SourceGap, match="generation"):
        check_source(old, status(generation="00000000-0000-0000-0000-000000000003"), 1, expected_source="real")
    assert json.loads((old / "gap.json").read_bytes())["reason"] == "source_generation_changed"
    (old / "target.json").write_text(json.dumps(dict(lane="original", source="real")))
    new = tmp_path / "new"
    receipt = rebase(old, new, status(expired_through=1, earliest_available_seq=2), after=1,
                     reason="Test-only retention gap", identity=dict(lane="after_gap", source="real"))
    assert receipt["coverage"] == "starts_after_explicit_gap" and (old / "gap.json").exists()
    check_source(new, status(expired_through=1, earliest_available_seq=2), 1, expected_source="real")


def test_retained_cursor_is_not_a_readability_guarantee(tmp_path):
    with pytest.raises(SourceGap, match="retention"):
        check_source(tmp_path, status(expired_through=1, earliest_available_seq=2), 0, expected_source="real")


def test_real_sync_cleanup_after_offline_expiry_preserves_head_and_records_gap(tmp_path):
    row = generate(users=1)["events"][0] | {"source": "real", "accepted_at": AT.isoformat()}
    identity = dict(source="real", lane="test")
    def fail(_):
        raise RuntimeError("sink unavailable")
    with pytest.raises(RuntimeError):
        sync_once(tmp_path, lambda _: dict(events=[row], next_cursor=1), fail,
                  identity=identity, status=status, now=AT)
    assert (tmp_path / "pending.json").exists()
    with pytest.raises(SourceGap, match="expired"):
        sync_once(tmp_path, lambda _: pytest.fail("must not fetch"), lambda _: pytest.fail("must not publish"),
                  identity=identity, status=status, now=AT + timedelta(days=7))
    assert not list((tmp_path / "data/batches").glob("*.json"))
    assert (tmp_path / "source.json").exists() and (tmp_path / "target.json").exists()
    assert not (tmp_path / "cursor.json").exists()
