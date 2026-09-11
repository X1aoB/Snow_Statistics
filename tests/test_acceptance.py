from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from snow_statistics.contracts import Event
from snow_statistics.model import build, funnel
from snow_statistics.simulator import generate, identity
from snow_statistics.store import Store


def test_lite_offline_integer_reconciliation_at_shared_cutoff(settings):
    fixture = generate(users=30)
    events = [Event.model_validate(r["event"]) for r in fixture["events"]]
    # Real-labelled fixtures exist only in this temporary isolated test database.
    now = max(e.occurred_at for e in events) + timedelta(seconds=1)
    store = Store(settings, clock=lambda: now)
    for start in range(0, len(events), 50):
        store.ingest(events[start:start + 50])
    while store.aggregate():
        pass
    rows = build(fixture)["daily"]
    expected = [{k: v for k, v in row.items() if k != "source"} for row in rows]
    actual = [{k: v for k, v in row.model_dump().items() if k != "success_rate"} for row in store.summary().daily]
    assert actual == expected
    store.close()


def test_latest_click_once_no_fallback_to_old_click():
    fixture = generate(users=2)
    rows = [r for r in fixture["events"] if r["event"].get("anonymous_id") in
            {identity(42, "mywebsite/1"), identity(42, "project_snow/1")} or r["event"].get("request_id") == identity(42, "request/1/0")]
    click = next(r for r in rows if r["event"]["event_type"] == "entry_click")
    arrival = next(r for r in rows if r["event"]["event_type"] == "entry_arrival")
    new_jump = str(uuid4())
    later = click | {"event": click["event"] | {"event_id": str(uuid4()), "jump_id": new_jump, "occurred_at": "2026-01-01T00:00:04.000Z"}}
    reached = arrival | {"event": arrival["event"] | {"event_id": str(uuid4()), "jump_id": new_jump, "occurred_at": "2026-01-01T00:00:05.000Z"}}
    rows.extend([later, reached])
    result = funnel(rows)
    assert len(result) == 1 and result[0]["jump_id"] == new_jump
    complete = next(r for r in rows if r["event"]["event_type"] == "request_complete")
    observed = next(r for r in rows if r["event"]["event_type"] == "request_observed")
    rows.extend([complete | {"event": complete["event"] | {"event_id": str(uuid4()), "request_id": "second"}},
                 observed | {"event": observed["event"] | {"event_id": str(uuid4()), "request_id": "second"}}])
    assert len(funnel(rows)) == 1


def test_utc_to_hong_kong_midnight_and_no_cross_app_id_merge(settings):
    now = datetime(2026, 9, 11, 16, tzinfo=UTC)
    visitor = uuid4()
    events = [Event(event_id=uuid4(), app="mywebsite", event_type="page_view", path="/", anonymous_id=visitor,
                    occurred_at=now - timedelta(milliseconds=1)),
              Event(event_id=uuid4(), app="mywebsite", event_type="page_view", path="/", anonymous_id=visitor, occurred_at=now)]
    store = Store(replace(settings, db=settings.db.parent / "midnight.db"), clock=lambda: now)
    store.ingest(events)
    store.aggregate()
    assert [(r.date, r.uv) for r in store.summary().daily] == [("2026-09-11", 1), ("2026-09-12", 1)]
    store.close()
