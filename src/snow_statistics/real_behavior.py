"""Pure Python 3.8-compatible contracts shared by the real Spark driver/tests."""
import re
from collections import defaultdict
from datetime import datetime, time, timedelta, timezone
from uuid import UUID

HK = timezone(timedelta(hours=8))
AUX_FIELDS = ("source", "seq", "event_id", "app", "event_type", "occurred_at", "accepted_at",
              "anonymous_id", "jump_id", "channel", "request_id", "success")


def stamp(value):
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("Timestamp needs timezone")
    return result.astimezone(timezone.utc)


def validate_coverage(value, cutoff, identity=None):
    if set(value) != {"schema_version", "source", "instance_id", "generation", "continuous_from", "through", "gaps"}:
        raise ValueError("Invalid real coverage fields")
    if value["schema_version"] != 1 or value["source"] != "real":
        raise ValueError("Coverage must describe real accepted events")
    for key in ("instance_id", "generation"):
        UUID(value[key])
        if identity and value[key] != identity[key]:
            raise ValueError("Coverage source generation changed")
    start, end = stamp(value["continuous_from"]), stamp(value["through"])
    if start > end or end != stamp(cutoff):
        raise ValueError("Coverage must end at the exact model cutoff")
    if not isinstance(value["gaps"], list) or len(value["gaps"]) > 1000:
        raise ValueError("Unbounded gap metadata")
    for gap in value["gaps"]:
        if set(gap) != {"from", "to", "reason"} or not re.fullmatch(r"[a-z0-9_-]{1,80}", gap["reason"]):
            raise ValueError("Invalid gap metadata")
        if not start <= stamp(gap["from"]) <= stamp(gap["to"]) <= end:
            raise ValueError("Gap outside coverage window")
    return value


def observation_status(cohort_day, lag, cutoff, coverage, retained_from):
    start = datetime.combine(datetime.fromisoformat(cohort_day).date(), time(), HK)
    target_end = start + timedelta(days=lag + 1)
    if target_end > stamp(cutoff):
        return "pending"
    # This cohort means first observed in the retained observation window; it is
    # never described as a first lifetime visit or a new natural person.
    if start < max(stamp(coverage["continuous_from"]), stamp(retained_from)):
        return "incomplete"
    if any(stamp(gap["from"]) < stamp(cutoff) and stamp(gap["to"]) >= start for gap in coverage["gaps"]):
        return "incomplete"
    return "complete_accepted_prefix"


def real_path(value, category):
    prefixes = {"warehouse": "/snow/warehouse/real/", "auxiliary": "/snow/auxiliary/real/"}
    pattern = r"hdfs://[a-zA-Z0-9.-]+:9000" + re.escape(prefixes[category]) + r"[A-Za-z0-9_/-]{1,200}"
    if not re.fullmatch(pattern, value) or "/../" in value or value.endswith("/.."):
        raise ValueError("Resource path must belong to the real namespace")
    return value


def auxiliary_manifest(path, coverage, original_min_accepted_at, rows, retained_from):
    real_path(path, "auxiliary")
    if type(rows) is not int or rows < 0:
        raise ValueError("Invalid auxiliary row count")
    return dict(schema_version=1, source="real", kind="auxiliary", path=path, rows=rows,
                instance_id=coverage["instance_id"], generation=coverage["generation"],
                original_min_accepted_at=original_min_accepted_at,
                expires_at=(stamp(original_min_accepted_at) + timedelta(days=30)).isoformat() if rows else None,
                fields=list(AUX_FIELDS), cutoff=coverage["through"], retained_from=retained_from,
                retention="original accepted_at plus 30 days; startup prune before reads")


def validate_auxiliary_manifest(value, coverage, now):
    if value["schema_version"] != 1 or value["source"] != "real" or value["kind"] != "auxiliary" or value["fields"] != list(AUX_FIELDS):
        raise ValueError("Unexpected auxiliary contract")
    real_path(value["path"], "auxiliary")
    for key in ("instance_id", "generation"):
        if value[key] != coverage[key]:
            raise ValueError("Auxiliary state belongs to a different source generation")
    if stamp(value["retained_from"]) > stamp(value["cutoff"]):
        raise ValueError("Invalid auxiliary observation window")
    if value["rows"]:
        if stamp(value["expires_at"]) != stamp(value["original_min_accepted_at"]) + timedelta(days=30):
            raise ValueError("Copying auxiliary state cannot extend expiry")
        if stamp(value["expires_at"]) <= now:
            raise ValueError("Expired auxiliary state: prune before model reads")
    return value


def prune_tokens(rows, now):
    """Cleanup-only adapter: original dates survive copying, replay and pruning."""
    kept = []
    for row in rows:
        if set(row) != set(AUX_FIELDS) or row["source"] != "real":
            raise ValueError("Unexpected auxiliary fields/source")
        if stamp(row["accepted_at"]) + timedelta(days=30) > now:
            kept.append(dict(row))
    return kept


def merge_tokens(previous, envelopes, now):
    """Independent bounded fixture oracle; production union/dedup runs in Spark."""
    if len(previous) > 100000 or len(envelopes) > 100000:
        raise ValueError("Fixture oracle input too large")
    if len(prune_tokens(previous, now)) != len(previous):
        raise ValueError("Expired auxiliary input must be pruned before model reads")
    combined = list(previous)
    for envelope in envelopes:
        if envelope["source"] != "real":
            raise ValueError("Real auxiliary state cannot mix sources")
        e = envelope["event"]
        row = {name: e.get(name) for name in AUX_FIELDS}
        row.update(source="real", seq=envelope["seq"], accepted_at=envelope["accepted_at"])
        if row["success"] is not None:
            row["success"] = str(row["success"]).lower()
        combined.append(row)
    selected = {}
    for row in sorted(prune_tokens(combined, now), key=lambda r: (stamp(r["accepted_at"]), r["seq"])):
        key = (row["app"], "request:" + row["request_id"] if row["event_type"] == "request_complete" else "event:" + row["event_id"])
        selected.setdefault(key, row)
    return list(selected.values())


def retention_oracle(tokens, date_from, date_to, cutoff, coverage, retained_from):
    """Small independent hand-check oracle, including unavailable denominators."""
    validate_coverage(coverage, cutoff)
    through = stamp(cutoff).astimezone(HK).date() - timedelta(days=1)
    people = defaultdict(set)
    for row in tokens:
        if row["source"] != "real":
            raise ValueError("Real oracle source mismatch")
        if row["anonymous_id"] and stamp(row["accepted_at"]) <= stamp(cutoff):
            day = stamp(row["occurred_at"]).astimezone(HK).date()
            if day <= through:
                people[(row["app"], row["anonymous_id"])].add(day)
    cohorts = defaultdict(list)
    for (app, _), days in people.items():
        first = min(days)
        if date_from <= first.isoformat() <= date_to:
            cohorts[(app, first)].append(days)
    result = []
    for (app, first), users in sorted(cohorts.items()):
        row = dict(source="real", app=app, cohort_date=first.isoformat(), users=len(users))
        for lag in (1, 7):
            state = observation_status(first.isoformat(), lag, cutoff, coverage, retained_from)
            row["observation_d" + str(lag)] = state
            row["eligible_d" + str(lag)] = len(users) if state == "complete_accepted_prefix" else 0
            row["retained_d" + str(lag)] = sum(first + timedelta(days=lag) in days for days in users) if state == "complete_accepted_prefix" else None
        result.append(row)
    return result
