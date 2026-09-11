"""Seeded, clocked synthetic-only fixtures. No business product connectivity."""
import random
from datetime import UTC, datetime, timedelta
from uuid import NAMESPACE_URL, uuid5

from .contracts import instant


def identity(seed, label):
    return str(uuid5(NAMESPACE_URL, f"snow-statistics/{seed}/{label}"))


def generate(seed=42, users=100, start=datetime(2026, 1, 1, tzinfo=UTC)):
    rng = random.Random(seed)
    rows, changes = [], []

    def event(app, kind, when, **fields):
        value = {"schema_version": 1, "event_id": identity(seed, f"event/{len(rows)}"), "app": app,
                 "event_type": kind, "occurred_at": instant(when), **fields}
        rows.append({"seq": len(rows) + 1, "source": "synthetic", "accepted_at": instant(when + timedelta(seconds=1)), "event": value})

    def change(table, key, when, after):
        changes.append({"source": "synthetic", "table": table, "key": key, "at": instant(when),
                        "version": len(changes) + 1, "op": "d" if after is None else "u", "after": after})

    change("contents", "content-1", start, {"title": "Synthetic article", "category": "data", "status": "published"})
    change("contents", "content-1", start + timedelta(days=1), {"title": "Synthetic article", "category": "engineering", "status": "published"})
    change("contents", "content-2", start, {"title": "Retired example", "category": "data", "status": "published"})
    change("contents", "content-2", start + timedelta(days=2), None)
    change("campaigns", "portfolio", start, {"name": "Portfolio entry", "status": "active"})
    for user in range(users):
        website = identity(seed, f"mywebsite/{user}")
        snow = identity(seed, f"project_snow/{user}")
        for day in range(3 if user % 3 == 0 else 1):
            at = start + timedelta(days=day, seconds=user * 2)
            session = identity(seed, f"session/{user}/{day}")
            jump = identity(seed, f"jump/{user}/{day}")
            request = identity(seed, f"request/{user}/{day}")
            event("mywebsite", "page_view", at, anonymous_id=website, session_id=session, path="/")
            event("mywebsite", "entry_click", at + timedelta(seconds=1), anonymous_id=website, session_id=session, jump_id=jump, channel="portfolio")
            event("project_snow", "entry_arrival", at + timedelta(seconds=2), anonymous_id=snow, session_id=identity(seed, f"snow-session/{user}/{day}"), jump_id=jump)
            event("project_snow", "page_view", at + timedelta(seconds=3), anonymous_id=snow, path="/")
            event("project_snow", "character_select", at + timedelta(seconds=4), anonymous_id=snow, character_id="sample_character")
            delay = 1900 if user % 10 == 0 else 10
            event("project_snow", "request_observed", at + timedelta(seconds=delay), anonymous_id=snow, request_id=request)
            event("project_snow", "request_complete", at + timedelta(seconds=delay + 1), character_id="sample_character", request_id=request, success=rng.random() > .1, elapsed_ms=rng.randint(100, 4000))
        ticket = f"ticket-{user}"
        for offset, status in [(0, "open"), (10, "in_progress"), (60, "resolved"), (90, "open"), (120, "resolved")]:
            change("tickets", ticket, start + timedelta(minutes=offset, seconds=user), {"status": status, "category": "synthetic_feedback"})
    return {"schema_version": 1, "source": "synthetic", "seed": seed, "start": instant(start), "events": rows, "changes": changes}
