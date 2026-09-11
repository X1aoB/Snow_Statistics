"""Streaming fixed-clock, synthetic scale input; expected integers use a separate loop."""
from datetime import UTC, datetime, timedelta
from uuid import NAMESPACE_URL, uuid5

from .contracts import instant


def events(groups, seed=42):
    def uid(label):
        return str(uuid5(NAMESPACE_URL, f"snow-scale/{seed}/{label}"))
    for group in range(groups):
        at = datetime(2026, 1, 1, tzinfo=UTC) + timedelta(days=group % 7, seconds=group // 7)
        web, snow, jump = uid(f"web/{group % 1000}"), uid(f"snow/{group % 800}"), uid(f"jump/{group}")
        request = f"req_{group}"
        char = "hot_character" if group % 20 else "other_character"
        shapes = [dict(app="mywebsite", event_type="page_view", path="/", anonymous_id=web),
                  dict(app="mywebsite", event_type="entry_click", jump_id=jump, channel="portfolio", anonymous_id=web),
                  dict(app="project_snow", event_type="entry_arrival", jump_id=jump, anonymous_id=snow),
                  dict(app="project_snow", event_type="page_view", path="/", anonymous_id=snow),
                  dict(app="project_snow", event_type="character_select", character_id=char, anonymous_id=snow),
                  dict(app="project_snow", event_type="request_observed", request_id=request, anonymous_id=snow),
                  dict(app="project_snow", event_type="request_complete", request_id=request, character_id=char,
                       success=group % 4 != 0, elapsed_ms=100 + group % 500),
                  dict(app="mywebsite", event_type="page_view", path="/statistics/", anonymous_id=web),
                  dict(app="project_snow", event_type="character_select", character_id=char, anonymous_id=snow)]
        completion = None
        for j, shape in enumerate(shapes):
            stamp = at + timedelta(seconds=j)
            event = dict(schema_version=1, event_id=uid(f"event/{group}/{j}"), occurred_at=instant(stamp), **shape)
            if j == 6:
                completion = event
            yield dict(seq=group * 10 + j + 1, source="synthetic", accepted_at=instant(stamp + timedelta(seconds=1)), event=event)
        # Even groups replay the identical event; odd groups repeat the request
        # under a new event ID on the next day, with the opposite outcome.
        duplicate = dict(completion)
        stamp = at + timedelta(days=1, seconds=10)
        if group % 2:
            duplicate.update(event_id=uid(f"event/{group}/9"), occurred_at=instant(stamp), success=not completion["success"])
        yield dict(seq=group * 10 + 10, source="synthetic", accepted_at=instant(stamp + timedelta(seconds=1)), event=duplicate)


def expected(groups):
    daily = {}
    visitors = {}
    for group in range(groups):
        day = f"2026-01-{group % 7 + 1:02d}"
        for app, pv, requests, successes, visitor in (
                ("mywebsite", 2, 0, 0, group % 1000),
                ("project_snow", 1, 1, int(group % 4 != 0), group % 800)):
            key = (day, app)
            row = daily.setdefault(key, dict(source="synthetic", date=day, app=app, pv=0, uv=0, requests=0, successes=0))
            row["pv"] += pv
            row["requests"] += requests
            row["successes"] += successes
            visitors.setdefault(key, set()).add(visitor)
    for key, row in daily.items():
        row["uv"] = len(visitors[key])
    return dict(daily=[daily[key] for key in sorted(daily)],
                quality=dict(raw=groups * 10, valid=groups * 9, duplicates=groups, quarantined=0, after_cutoff=0))
