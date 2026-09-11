"""Independent, small-data oracle for bounded session/retention/entry attribution.

Only test fixtures use this loop implementation. Distributed work lives in Spark.
"""
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta

from .contracts import business_day
from .model import deduplicate


def stamp(value):
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("Timestamp requires timezone")
    return result.astimezone(UTC)


def analyze(rows, date_from, date_to, cutoff):
    end = stamp(cutoff)
    closed_day = date.fromisoformat(business_day(end)) - timedelta(days=1)
    if not date.fromisoformat(date_from) <= date.fromisoformat(date_to) <= closed_day:
        raise ValueError("Report dates must be closed HK business dates at cutoff")
    valid, _, _ = deduplicate([r for r in rows if stamp(r["accepted_at"]) <= end])
    events = [(r["source"], r["event"]) for r in valid if stamp(r["event"]["occurred_at"]) <= end]
    events.sort(key=lambda pair: (stamp(pair[1]["occurred_at"]), pair[1]["event_id"]))
    activity = defaultdict(list)
    for source, e in events:
        if e.get("anonymous_id"):
            activity[(source, e["app"], e["anonymous_id"])].append(stamp(e["occurred_at"]))
    sessions, cohorts = [], defaultdict(list)
    for (source, app, anon), times in activity.items():
        groups = [[times[0]]]
        for at in times[1:]:
            if at - groups[-1][-1] >= timedelta(minutes=30):
                groups.append([])
            groups[-1].append(at)
        for group in groups:
            day = business_day(group[0])
            if date_from <= day <= date_to:
                sessions.append(dict(source=source, app=app, anonymous_id=anon, date=day,
                                     start=group[0].isoformat(), end=group[-1].isoformat(), events=len(group),
                                     duration_seconds=(group[-1] - group[0]).total_seconds(),
                                     closed=group[-1] + timedelta(minutes=30) <= end))
        days = {business_day(t) for t in times if business_day(t) <= str(closed_day)}
        if days:
            first = min(days)
            if date_from <= first <= date_to:
                cohorts[(source, app, first)].append(days)
    retention = []
    for (source, app, first), people in cohorts.items():
        row = dict(source=source, app=app, cohort_date=first, users=len(people))
        for lag in (1, 7):
            target = date.fromisoformat(first) + timedelta(days=lag)
            mature = target <= closed_day
            row[f"eligible_d{lag}"] = len(people) if mature else 0
            row[f"retained_d{lag}"] = sum(str(target) in days for days in people) if mature else None
        retention.append(row)
    daily = defaultdict(list)
    for s in sessions:
        daily[(s["source"], s["app"], s["date"])].append(s)
    session_daily = [dict(source=k[0], app=k[1], date=k[2], sessions=len(values),
                          users=len({v["anonymous_id"] for v in values}), events=sum(v["events"] for v in values),
                          duration_seconds=sum(v["duration_seconds"] for v in values),
                          closed_sessions=sum(v["closed"] for v in values)) for k, values in daily.items()]
    clicks, observed, completions = {}, {}, []
    arrivals, selections = defaultdict(list), defaultdict(list)
    for source, e in events:
        kind = e["event_type"]
        if kind == "entry_click":
            clicks.setdefault((source, e["jump_id"]), e)
        elif kind == "entry_arrival" and e.get("anonymous_id"):
            arrivals[(source, e["jump_id"])].append(e)
        elif kind == "request_observed" and e.get("anonymous_id"):
            observed.setdefault((source, e["request_id"]), e)
        elif kind == "request_complete" and e["success"]:
            completions.append((source, e))
        elif kind == "character_select" and e.get("anonymous_id"):
            selections[(source, e["anonymous_id"])].append(e)
    journeys = {}
    for key, click in clicks.items():
        start = stamp(click["occurred_at"])
        arrival = next((a for a in arrivals[key] if start <= stamp(a["occurred_at"]) <= start + timedelta(minutes=30)), None)
        journeys[key] = (click, arrival)
    conversions, consumed = [], set()
    for source, complete in sorted(completions, key=lambda pair: (stamp(pair[1]["occurred_at"]), pair[1]["request_id"])):
        observed_event = observed.get((source, complete["request_id"]))
        if observed_event is None:
            continue
        candidates = []
        for key, (click, arrival) in journeys.items():
            if key[0] != source or not arrival or arrival["anonymous_id"] != observed_event["anonymous_id"]:
                continue
            start = stamp(click["occurred_at"])
            if start <= stamp(arrival["occurred_at"]) <= stamp(observed_event["occurred_at"]) <= stamp(complete["occurred_at"]) <= start + timedelta(minutes=30):
                candidates.append((start, key[1], key))
        if candidates:
            key = max(candidates)[2]
            if key not in consumed:
                consumed.add(key)
                click = clicks[key]
                conversions.append(dict(source=source, jump_id=key[1], request_id=complete["request_id"],
                                        channel=click["channel"], date=business_day(stamp(click["occurred_at"]))))
    funnel = defaultdict(lambda: dict(clicks=0, arrived=0, selected=0, requested=0, converted=0))
    for key, (click, arrival) in journeys.items():
        day = business_day(stamp(click["occurred_at"]))
        if not date_from <= day <= date_to:
            continue
        row = funnel[(key[0], click["channel"], day)]
        row["clicks"] += 1
        row["converted"] += key in consumed
        if arrival:
            row["arrived"] += 1
            lower, upper = stamp(arrival["occurred_at"]), stamp(click["occurred_at"]) + timedelta(minutes=30)
            row["selected"] += any(lower <= stamp(e["occurred_at"]) <= upper for e in selections[(key[0], arrival["anonymous_id"])])
            row["requested"] += any(s == key[0] and e["anonymous_id"] == arrival["anonymous_id"] and lower <= stamp(e["occurred_at"]) <= upper for (s, _), e in observed.items())
    return dict(sessions=sessions, session_daily=session_daily, retention=retention,
                conversions=[r for r in conversions if date_from <= r["date"] <= date_to],
                funnel=[dict(source=k[0], channel=k[1], date=k[2], **v) for k, v in funnel.items()],
                complete_through=str(closed_day), cohort_definition="first_observed_in_input")
