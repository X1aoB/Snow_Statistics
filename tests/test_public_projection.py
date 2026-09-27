"""Synthetic fixtures exercise production-labelled isolation without real users."""
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from snow_statistics import public_projection
from snow_statistics.api import create_app
from snow_statistics.contracts import Event, PublicSummary, Summary
from snow_statistics.store import AggregateGap, CursorExpired, Store

PUBLICATION = datetime(2026, 9, 13, 16, 15, tzinfo=UTC)  # Sept 11 + 3, 00:15 Hong Kong.


def pages(page, count, **updates):
    return [page.model_copy(update={"event_id": uuid4(), "anonymous_id": uuid4(), **updates}) for _ in range(count)]


def completions(now, successes, failures):
    return [Event(event_id=uuid4(), app="project_snow", event_type="request_complete", occurred_at=now,
                  request_id=f"request-{i}", character_id="sample_character", success=i < successes, elapsed_ms=10)
            for i in range(successes + failures)]


def published(store):
    store.clock = lambda: PUBLICATION
    store.aggregate()
    return store.public_summary_v2()


def day(snapshot, app="mywebsite", label="2026-09-11"):
    return next(row for row in snapshot.daily if row.app == app and row.date == label)


def test_fixed_hong_kong_cutoff_pending_then_independent_groups(store, page, now):
    store.ingest(pages(page, 10, app="project_snow") + completions(now, 9, 1))
    store.aggregate()
    assert day(store.public_summary_v2(), "project_snow").access.state == "pending"
    store.clock = lambda: PUBLICATION - timedelta(seconds=1)
    store.aggregate()
    assert day(store.public_summary_v2(), "project_snow").access.state == "pending"
    value = day(published(store), "project_snow")
    assert value.access.model_dump() == {"state": "published", "value": {"pv": 10, "uv": 10}}
    assert value.quality.model_dump() == {"state": "suppressed", "value": None}
    assert value.popularity.model_dump() == {"state": "published", "value": [{"kind": "page", "name": "/", "count": 10}]}
    assert value.cutoff_at == "2026-09-13T16:15:00.000Z"
    assert not [row for row in store.public_summary_v1().daily if row.app == "project_snow" and row.date == "2026-09-11"]
    assert sum(row.requests for row in store.summary().daily) == 10


@pytest.mark.parametrize("successes,failures,expected", [(0, 0, "empty"), (9, 0, "suppressed"),
                         (10, 0, "published"), (0, 10, "published"), (10, 1, "suppressed"),
                         (1, 10, "suppressed"), (10, 9, "suppressed"), (10, 10, "published")])
def test_quality_thresholds(store, now, successes, failures, expected):
    store.ingest(completions(now, successes, failures))
    store.aggregate()
    group = day(published(store), "project_snow").quality
    assert group.state == expected
    if expected == "published":
        assert group.value.requests == successes + failures
        assert group.value.success_rate == successes / (successes + failures)
    else:
        assert group.value is None


def test_category_distinct_counts_and_whole_heat_suppression(store, page):
    first = pages(page, 10)
    rare = page.model_copy(update={"event_id": uuid4(), "path": "/statistics/"})
    store.ingest(first + [rare])
    store.aggregate()
    value = day(published(store))
    assert value.access.value.pv == 11 and value.access.value.uv == 11
    assert value.popularity.state == "suppressed" and value.popularity.value is None
    legacy = store.public_summary_v1()
    assert next(row for row in legacy.daily if row.app == "mywebsite" and row.date == "2026-09-11").pv == 11
    assert not legacy.popularity


def test_repeating_an_identifier_does_not_meet_public_threshold(store, page):
    store.ingest(pages(page, 20, anonymous_id=page.anonymous_id))
    store.aggregate()
    value = day(published(store))
    assert value.access.state == value.popularity.state == "suppressed"
    assert store.summary().daily[0].pv == 20 and store.summary().daily[0].uv == 1


def test_late_data_cannot_change_frozen_values_or_retroactively_publish(store, page):
    store.ingest(pages(page, 10))
    store.aggregate()
    before = day(published(store)).model_dump()
    store.ingest(pages(page, 10))  # Same business date, accepted at/after its cutoff.
    store.aggregate()
    store.clock = lambda: PUBLICATION + timedelta(days=1)
    store.aggregate()
    assert day(store.public_summary_v2()).model_dump() == before
    assert store.summary().daily[0].pv == 20


def test_delayed_worker_still_uses_original_acceptance_cutoff(store, page):
    store.ingest(pages(page, 9))
    # No aggregate worker ran before cutoff. Nine events remain eligible; late
    # events cannot raise the publication sample to ten when it eventually runs.
    store.clock = lambda: PUBLICATION + timedelta(hours=1)
    store.ingest(pages(page, 1))
    assert store.publish_public() is False  # An incomplete prefix cannot freeze.
    store.aggregate()
    assert day(store.public_summary_v2()).access.state == "suppressed"
    assert store.summary().daily[0].uv == 10


def test_duplicate_reopen_and_auxiliary_expiry_keep_frozen_snapshot(store, page, settings, now):
    events = pages(page, 10)
    store.ingest(events)
    store.ingest(events)
    store.aggregate()
    before = day(published(store)).model_dump()
    identity = store.sync_status()
    reopened = Store(settings, clock=lambda: PUBLICATION)
    try:
        assert reopened.sync_status()["generation"] == identity["generation"]
        assert reopened.sync_status()["instance_id"] == identity["instance_id"]
        assert day(reopened.public_summary_v2()).model_dump() == before
        reopened.clock = lambda: now + timedelta(days=31)
        reopened.maintain()
        assert reopened.db.execute("SELECT COUNT(*) FROM public_visitors").fetchone()[0] == 0
        assert reopened.db.execute("SELECT COUNT(*) FROM public_popularity_visitors").fetchone()[0] == 0
        assert day(reopened.public_summary_v2()).model_dump() == before
        status = reopened.sync_status()
        assert status["latest_accepted_seq"] == status["expired_through"] == 10
        assert status["earliest_available_seq"] is None
    finally:
        reopened.close()


def test_failed_snapshot_preserves_timestamp_but_exact_aggregation_commits(store, page, monkeypatch):
    store.ingest(pages(page, 10))
    store.aggregate()
    before = store.public_summary_v2().model_dump()
    store.clock = lambda: PUBLICATION
    store.ingest(pages(page, 1))
    original = public_projection.publish
    def broken(db, now, **kwargs):
        return original(db, now, fail_before_commit=True)
    monkeypatch.setattr(public_projection, "publish", broken)
    with pytest.raises(RuntimeError, match="public snapshot"):
        store.aggregate()
    assert store.summary().daily[0].pv == 11
    snapshot = store.public_summary_v2()
    assert snapshot.generated_at == before["generated_at"]
    assert snapshot.status == "stale"
    assert snapshot.daily == PublicSummary.model_validate(before).daily
    assert store.db.execute("SELECT COUNT(*) FROM public_frozen").fetchone()[0] == 0


def test_public_v1_v2_no_bypass_private_auth_and_off_archive(store, settings, page):
    store.ingest(pages(page, 9))
    store.aggregate()
    published(store)
    for mode in ("full", "lite", "off"):
        with TestClient(create_app(replace(settings, mode=mode), store)) as client:
            one = Summary.model_validate(client.get("/analytics/public/v1/summary.json").json())
            two = PublicSummary.model_validate(client.get("/analytics/public/v2/summary.json").json())
            assert not [row for row in one.daily if row.app == "mywebsite" and row.date == "2026-09-11"]
            assert day(two).access.value is None
            payload = two.model_dump_json()
            for forbidden in (str(page.anonymous_id), "anonymous_id", "request_id", "event_id", "elapsed_ms", "source", "visitors"):
                assert forbidden not in payload
            for endpoint in ("summary.json", "status"):
                url = "/analytics/private/v1/" + endpoint
                assert client.get(url).status_code == 401
                assert client.get(url, headers={"Authorization": "Bearer server-test-only"}).status_code == 401
                private = client.get(url, headers={"Authorization": "Bearer reader-test-only"})
                assert private.status_code == 200 and private.headers["cache-control"] == "no-store"
            if mode == "off":
                assert one.status == two.status == "archived"


def test_synthetic_counts_and_legacy_history_never_leak(settings, now, page):
    synthetic = Store(replace(settings, source="synthetic"), clock=lambda: now)
    try:
        synthetic.ingest(pages(page, 20))
        synthetic.aggregate()
        assert day(published(synthetic)).access.state == "empty"
        assert not any(row.pv for row in synthetic.public_summary_v1().daily)
    finally:
        synthetic.close()


def test_expired_snapshot_values_are_removed_without_faking_generation(store, page, now):
    store.ingest(pages(page, 10))
    store.aggregate()
    published(store)
    generated = store.public_summary_v2().generated_at
    store.clock = lambda: now + timedelta(days=100)
    assert store.summary().daily == []
    store.maintain()
    assert store.public_summary_v2().daily == []
    assert store.public_summary_v2().generated_at == generated
    saved = json.loads(store.db.execute("SELECT payload FROM public_snapshots").fetchone()[0])
    assert saved["daily"] == []
    assert store.db.execute("SELECT COUNT(*) FROM public_frozen").fetchone()[0] == 0
    assert json.loads(store.db.execute("SELECT payload FROM snapshots").fetchone()[0])["daily"] == []
    assert store.db.execute("PRAGMA secure_delete").fetchone()[0] == 1


def test_raw_expiry_is_enforced_even_when_worker_is_stopped(store, page, now):
    store.ingest([page])
    store.aggregate()
    store.clock = lambda: now + timedelta(days=7)
    with pytest.raises(CursorExpired):
        store.read()
    status = store.sync_status()
    assert status["expired_through"] == 1 and status["earliest_available_seq"] is None
    assert status["aggregate_gap"] is None
    assert store.db.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1  # Read barrier is independent of physical cleanup.


def test_unprocessed_expiry_is_deleted_but_blocks_silent_zero_publication(store, page, now):
    store.aggregate()
    before = store.public_summary_v2().generated_at
    store.ingest([page])
    store.clock = lambda: now + timedelta(days=8)
    assert store.sync_status()["aggregate_gap"]["expired_events"] == 1
    assert store.public_summary_v2().status == "unavailable"
    with pytest.raises(AggregateGap):
        store.aggregate()
    with pytest.raises(AggregateGap):
        store.maintain()
    assert store.db.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0
    assert store.public_summary_v2().generated_at == before
    assert store.sync_status()["aggregate_cursor"] == 0
    assert store.sync_status()["aggregate_gap"]["recovery_required"] is True
    with pytest.raises(AggregateGap):
        store.aggregate()
