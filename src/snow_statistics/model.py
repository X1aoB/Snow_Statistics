"""Small-data correctness oracle for Spark/Flink acceptance, not a scale claim."""
from collections import defaultdict
from datetime import datetime, timedelta

from .contracts import Event, business_day


def deduplicate(rows):
    seen, requests, valid, quarantine = {}, set(), [], []
    duplicates = 0
    for row in sorted(rows, key=lambda r: (r.get("accepted_at", ""), r.get("seq", 0))):
        try:
            event = Event.model_validate(row["event"])
            if row["source"] not in {"real", "synthetic"}:
                raise ValueError("invalid source")
        except (ValueError, KeyError):
            quarantine.append({"seq": row.get("seq"), "reason": "invalid_contract"})
            continue
        key = (row["source"], event.app, str(event.event_id))
        serialized = event.model_dump_json(exclude_none=True)
        if key in seen:
            if seen[key] != serialized:
                quarantine.append({"seq": row.get("seq"), "reason": "event_id_conflict"})
            else:
                duplicates += 1
            continue
        seen[key] = serialized
        if event.event_type == "request_complete":
            key = (row["source"], event.app, event.request_id)
            if key in requests:
                duplicates += 1
                continue
            requests.add(key)
        valid.append(row)
    return valid, {"raw": len(rows), "valid": len(valid), "duplicates": duplicates, "quarantined": len(quarantine)}, quarantine


def daily_metrics(rows):
    metrics, visitors = {}, defaultdict(set)
    for row in rows:
        e = row["event"]
        key = (row["source"], e["app"], business_day(datetime.fromisoformat(e["occurred_at"])))
        out = metrics.setdefault(key, dict(source=key[0], app=key[1], date=key[2], pv=0, uv=0, requests=0, successes=0))
        out["pv"] += e["event_type"] == "page_view"
        if e.get("anonymous_id"):
            visitors[key].add(e["anonymous_id"])
        if e["event_type"] == "request_complete":
            out["requests"] += 1
            out["successes"] += e["success"]
    for key, out in metrics.items():
        out["uv"] = len(visitors[key])
    return sorted(metrics.values(), key=lambda r: (r["source"], r["date"], r["app"]))


def scd2(changes, table="contents"):
    groups = defaultdict(dict)
    for c in changes:
        if c["table"] == table:
            groups[(c["source"], c["key"])][(c["at"], c["version"])] = c
    versions = []
    for (source, key), group in groups.items():
        ordered = sorted(group.values(), key=lambda c: (c["at"], c["version"]))
        # Last committed version wins when transactions share an effective timestamp.
        by_time = {c["at"]: c for c in ordered}
        ordered = list(by_time.values())
        for i, c in enumerate(ordered):
            versions.append(dict(source=source, key=key, valid_from=c["at"],
                                 valid_to=ordered[i + 1]["at"] if i + 1 < len(ordered) else None,
                                 deleted=c["op"] == "d", attributes=c["after"], version=c["version"]))
    return versions


def classify(versions, key, at=None, source="synthetic"):
    matches = [v for v in versions if v["source"] == source and v["key"] == key and
               (v["valid_to"] is None if at is None else v["valid_from"] <= at and (v["valid_to"] is None or at < v["valid_to"]))]
    return matches[0]["attributes"] if matches and not matches[0]["deleted"] else None


def ticket_snapshots(changes):
    groups, rounds, daily = defaultdict(dict), [], []
    for c in changes:
        if c["table"] == "tickets":
            groups[(c["source"], c["key"])][c["version"]] = c
    for (source, key), group in groups.items():
        ordered = sorted(group.values(), key=lambda c: (c["at"], c["version"]))
        active, completed, ticket_rounds = None, [], []
        for c in ordered:
            if c["op"] == "d":
                if active:
                    active["deleted_at"] = c["at"]
                active = None
                continue
            status = c["after"]["status"]
            if status == "open" and active is None:
                active = dict(source=source, ticket_id=key, round=len(completed) + 1, opened_at=c["at"], resolved_at=None)
                rounds.append(active)
                ticket_rounds.append(active)
            elif status == "resolved" and active:
                active["resolved_at"] = c["at"]
                active["duration_seconds"] = (datetime.fromisoformat(c["at"]) - datetime.fromisoformat(active["opened_at"])).total_seconds()
                completed.append(c["at"])
                active = None
        for round_ in ticket_rounds:
            round_["first_resolved_at"] = completed[0] if completed else None
            round_["latest_resolved_at"] = completed[-1] if completed else None
        first, last = [datetime.fromisoformat(ordered[i]["at"]) for i in (0, -1)]
        day = datetime.fromisoformat(business_day(first)).date()
        while day <= datetime.fromisoformat(business_day(last + timedelta(days=1))).date():
            observed = [c for c in ordered if business_day(datetime.fromisoformat(c["at"])) <= day.isoformat()]
            if observed:
                latest = observed[-1]
                daily.append(dict(source=source, ticket_id=key, date=day.isoformat(),
                                  status="deleted" if latest["op"] == "d" else latest["after"]["status"]))
            day += timedelta(days=1)
    return rounds, daily


def funnel(rows):
    clicks, arrivals, observed, selections, completions = {}, {}, [], defaultdict(list), []
    for row in rows:
        e, source = row["event"], row["source"]
        kind = e["event_type"]
        if kind == "entry_click":
            clicks.setdefault((source, e["jump_id"]), e)
        elif kind == "entry_arrival" and e.get("anonymous_id"):
            arrivals.setdefault((source, e["jump_id"]), e)
        elif kind == "character_select" and e.get("anonymous_id"):
            selections[(source, e["anonymous_id"])].append(e)
        elif kind == "request_observed" and e.get("anonymous_id"):
            observed.append((source, e))
        elif kind == "request_complete" and e["success"]:
            completions.append((source, e))
    observations = {(s, e["request_id"]): e for s, e in observed}
    arrival_index = defaultdict(list)
    for key, arrival in arrivals.items():
        arrival_index[(key[0], arrival["anonymous_id"])].append((key, arrival))
    result, used = [], set()
    for source, complete in sorted(completions, key=lambda item: item[1]["occurred_at"]):
        o = observations.get((source, complete["request_id"]))
        if not o:
            continue
        end = datetime.fromisoformat(complete["occurred_at"])
        candidates = []
        for key, arrival in arrival_index[(source, o["anonymous_id"])]:
            click = clicks.get(key)
            if key[0] != source or not click or arrival["anonymous_id"] != o["anonymous_id"]:
                continue
            start = datetime.fromisoformat(click["occurred_at"])
            if not start <= datetime.fromisoformat(arrival["occurred_at"]) <= datetime.fromisoformat(o["occurred_at"]) <= end <= start + timedelta(minutes=30):
                continue
            if not any(arrival["occurred_at"] <= e["occurred_at"] <= o["occurred_at"] for e in selections[(source, o["anonymous_id"])]):
                continue
            candidates.append((click["occurred_at"], key, click))
        if candidates:
            _, key, click = max(candidates)
            # Do not fall back to an older click when the latest already converted.
            if key not in used:
                used.add(key)
                result.append(dict(source=source, jump_id=key[1], channel=click["channel"], request_id=complete["request_id"]))
    return result


def activity(rows):
    groups = defaultdict(list)
    for row in rows:
        e = row["event"]
        if e.get("anonymous_id"):
            groups[(row["source"], e["app"], e["anonymous_id"])].append(datetime.fromisoformat(e["occurred_at"]))
    sessions, cohorts = [], defaultdict(lambda: [0, 0, 0])
    for key, times in groups.items():
        times.sort()
        start = previous = times[0]
        for at in times[1:]:
            if at - previous >= timedelta(minutes=30):
                sessions.append(dict(source=key[0], app=key[1], anonymous_id=key[2], start=start.isoformat(), end=previous.isoformat()))
                start = at
            previous = at
        sessions.append(dict(source=key[0], app=key[1], anonymous_id=key[2], start=start.isoformat(), end=previous.isoformat()))
        days = {business_day(t) for t in times}
        first = min(days)
        counts = cohorts[(key[0], key[1], first)]
        counts[0] += 1
        counts[1] += (datetime.fromisoformat(first) + timedelta(days=1)).date().isoformat() in days
        counts[2] += (datetime.fromisoformat(first) + timedelta(days=7)).date().isoformat() in days
    retention = [dict(source=k[0], app=k[1], cohort_date=k[2], users=v[0], retained_d1=v[1], retained_d7=v[2]) for k, v in cohorts.items()]
    return sessions, retention


def build(fixture):
    rows, quality, quarantine = deduplicate(fixture["events"])
    rounds, snapshots = ticket_snapshots(fixture.get("changes", []))
    sessions, retention = activity(rows)
    assert quality["raw"] == sum(quality[k] for k in ("valid", "duplicates", "quarantined"))
    return dict(quality=quality, quarantine=quarantine, daily=daily_metrics(rows),
                content_scd2=scd2(fixture.get("changes", [])), ticket_rounds=rounds, ticket_daily=snapshots,
                conversions=funnel(rows), sessions=sessions, retention=retention)
