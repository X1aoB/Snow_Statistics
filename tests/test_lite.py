import json
from dataclasses import replace
from datetime import timedelta
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from snow_statistics.api import create_app
from snow_statistics.contracts import Event, Summary
from snow_statistics.store import CursorExpired, EventConflict, StorageFull, Store


def test_ack_dedupe_reopen_and_atomic_aggregate(store, page, settings, now):
    assert store.ingest([page]) == {"accepted": 1, "duplicates": 0}
    assert store.ingest([page])["duplicates"] == 1
    with pytest.raises(RuntimeError):
        store.aggregate(fail_before_commit=True)
    assert store.db.execute("SELECT count(*) FROM daily").fetchone()[0] == 0
    store.aggregate()
    store.aggregate()
    assert store.summary().daily[0].pv == 1
    reopened = Store(settings, clock=lambda: now)
    assert reopened.read()["events"][0]["event"]["event_id"] == str(page.event_id)
    assert reopened.summary().daily[0].uv == 1
    reopened.close()


def test_id_conflict_rolls_back_whole_batch(store, page):
    store.ingest([page])
    another = page.model_copy(update={"event_id": uuid4()})
    conflict = page.model_copy(update={"anonymous_id": uuid4()})
    with pytest.raises(EventConflict):
        store.ingest([another, conflict])
    assert len(store.read()["events"]) == 1


def test_request_dedupe_cross_event_id_and_day(store, now):
    request = Event(event_id=uuid4(), app="project_snow", event_type="request_complete", occurred_at=now,
                    request_id="r1", character_id="sample_character", success=True, elapsed_ms=100)
    second = request.model_copy(update={"event_id": uuid4(), "occurred_at": now - timedelta(days=1)})
    store.ingest([request, second])
    store.aggregate()
    assert sum(r.requests for r in store.summary().daily) == 1
    assert sum(r.successes for r in store.summary().daily) == 1


def test_day_uv_and_app_scope(store, page, now):
    events = [page, page.model_copy(update={"event_id": uuid4()}),
              page.model_copy(update={"event_id": uuid4(), "app": "project_snow"}),
              page.model_copy(update={"event_id": uuid4(), "occurred_at": now - timedelta(days=1)})]
    store.ingest(events)
    store.aggregate()
    result = store.summary()
    assert len(result.daily) == 3
    assert all(r.uv == 1 for r in result.daily)
    assert sum(r.pv for r in result.daily) == 4


def test_capacity_reject_does_not_delete_confirmed(store, page, monkeypatch):
    store.ingest([page])
    monkeypatch.setattr(store, "disk_usage", lambda: store.settings.budget_bytes)
    with pytest.raises(StorageFull):
        store.ingest([page.model_copy(update={"event_id": uuid4()})])
    assert len(store.read()["events"]) == 1


def test_retention_cursor_and_auxiliary_expiry(store, page, now):
    store.ingest([page])
    store.aggregate()
    store.clock = lambda: now + timedelta(days=6)
    store.maintain()
    assert len(store.read()["events"]) == 1
    store.clock = lambda: now + timedelta(days=8)
    store.maintain()
    with pytest.raises(CursorExpired):
        store.read()
    assert store.read(after=1)["events"] == []
    store.clock = lambda: now + timedelta(days=31)
    store.maintain()
    assert store.db.execute("SELECT COUNT(*) FROM visitors").fetchone()[0] == 0
    assert store.db.execute("SELECT COUNT(*) FROM daily").fetchone()[0] == 1


def test_public_api_private_fields_origin_and_server_trust(settings, store, page, now):
    with TestClient(create_app(settings, store)) as client:
        body = {"events": [page.model_dump(mode="json", exclude_none=True)]}
        assert client.post("/analytics/v1/events", json=body).status_code == 403
        headers = {"Origin": "https://xiaob.dev"}
        assert client.post("/analytics/v1/events", json=body, headers=headers).status_code == 202
        assert client.get("/analytics/private/v1/events").status_code == 401
        assert client.get("/analytics/private/v1/events", headers={"Authorization": "Bearer reader-test-only"}).status_code == 200
        store.aggregate()
        public = client.get("/analytics/public/v1/summary.json").json()
        Summary.model_validate(public)
        serialized = json.dumps(public)
        for forbidden in (str(page.anonymous_id), str(page.event_id), "request_id", "elapsed_ms", "exception", "source"):
            assert forbidden not in serialized
        private = dict(body["events"][0], chat_body="never echo this")
        response = client.post("/analytics/v1/events", json={"events": [private]}, headers=headers)
        assert response.status_code == 422 and "never echo" not in response.text
        req = Event(event_id=uuid4(), app="project_snow", event_type="request_complete", occurred_at=now,
                    request_id="request1", character_id="sample_character", success=True, elapsed_ms=10)
        assert client.post("/analytics/v1/events", json={"events": [req.model_dump(mode="json")]}, headers=headers).status_code == 401
        response = client.post("/analytics/v1/events", json={"events": [req.model_dump(mode="json")]}, headers={"Authorization": "Bearer server-test-only"})
        assert response.status_code == 202
        assert client.post("/analytics/v1/events", content=b"x" * 65537, headers={"Content-Type": "application/json"}).status_code == 413


def test_off_and_lite_keep_identical_summary(settings, store, page):
    store.ingest([page])
    store.aggregate()
    expected = store.summary().daily
    for mode in ("full", "lite", "off"):
        with TestClient(create_app(replace(settings, mode=mode), store)) as client:
            result = Summary.model_validate(client.get("/analytics/public/v1/summary.json").json())
            assert result.daily == expected
            if mode == "off":
                assert result.status == "archived"
                assert client.post("/analytics/v1/events", json={}).status_code == 503


def test_synthetic_never_public(settings, page, now):
    synthetic = Store(replace(settings, db=settings.db.parent / "synthetic.db", source="synthetic"), clock=lambda: now)
    synthetic.ingest([page])
    synthetic.aggregate()
    assert synthetic.summary().daily == []
    synthetic.close()


def test_backlog_and_stale_status(store, page, now):
    store.ingest([page, page.model_copy(update={"event_id": uuid4()})])
    store.aggregate(limit=1)
    assert store.summary().status == "unavailable"
    store.aggregate(limit=1)
    store.clock = lambda: now + timedelta(minutes=4)
    assert store.summary().status == "stale"


def test_no_query_strings_and_no_timezone(store, page):
    with pytest.raises(ValueError):
        store.ingest([page.model_copy(update={"path": "/?secret=hidden"})])
    with pytest.raises(ValueError):
        Event.model_validate(page.model_dump() | {"occurred_at": "2026-09-11T12:00:00"})
