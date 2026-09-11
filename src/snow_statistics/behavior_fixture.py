"""Hand-inspectable boundaries: cross-day, 30 minutes, latest entry and maturity."""
from datetime import UTC, datetime, timedelta

from .contracts import instant
from .simulator import identity


def generate_behavior():
    rows = []
    start = datetime(2026, 1, 1, 15, 59, tzinfo=UTC)
    def emit(kind, second, person=None, **fields):
        app = "mywebsite" if kind in ("entry_click", "page_view") else "project_snow"
        if kind == "request_complete":
            fields = dict(character_id="sample_character", elapsed_ms=100, success=True) | fields
        if kind == "page_view":
            fields["path"] = "/"
        if person:
            fields["anonymous_id"] = identity(701, app + "/" + person)
        at = start + timedelta(seconds=second)
        event = dict(schema_version=1, event_id=identity(701, "event/" + str(len(rows))), app=app,
                     event_type=kind, occurred_at=instant(at), **fields)
        rows.append(dict(seq=len(rows) + 1, source="synthetic", accepted_at=instant(at + timedelta(seconds=1)), event=event))
    def click(label, at, person="a"):
        jump = identity(701, label)
        emit("entry_click", at, person, jump_id=jump, channel="portfolio")
        emit("entry_arrival", at + 1, person, jump_id=jump)
        return jump
    old = click("old-unused", 0)
    latest = click("latest", 10)
    emit("request_observed", 20, "a", request_id="first")
    emit("request_complete", 21, request_id="first")  # No role selection required.
    emit("request_observed", 22, "a", request_id="second")
    emit("request_complete", 23, request_id="second")  # Must not fall back to old-unused.
    exact = click("exact", 0, "b")
    emit("request_observed", 1799, "b", request_id="exact")
    emit("request_complete", 1800, request_id="exact")
    late = click("late", 0, "c")
    emit("request_observed", 1799, "c", request_id="late")
    emit("request_complete", 1800.001, request_id="late")
    click("failure", 0, "d")
    emit("character_select", 4, "d", character_id="sample_character")
    emit("request_observed", 10, "d", request_id="failed")
    emit("request_complete", 11, request_id="failed", success=False)
    for second in (0, 1799, 3599):
        emit("page_view", second, "session-boundary")
    for second in (86400, 7 * 86400):
        emit("page_view", second, "a")
    emit("page_view", 8 * 86400, "new-at-end")
    # Delivery duplicates and a new event_id for an already completed request.
    emit("request_complete", 24, request_id="first")
    rows += [dict(row) for row in rows[:3]]
    return dict(events=rows, cases=dict(old=old, latest=latest, exact=exact, late=late),
                date_from="2026-01-01", date_to="2026-01-09", cutoff="2026-01-09T16:00:00Z")
