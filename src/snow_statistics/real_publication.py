"""An event-only real release; no synthetic operations package is required."""
import json
import math
import re
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

from .io import digest, write_json
from .lifecycle import RealLifecycle
from .model_publication import FIELDS
from .publication import canonical, publication_lock, validate
from .real_behavior import HK, observation_status, real_path, stamp, validate_coverage

GROUPS = {"session_daily", "retention", "funnel"}
COUNTS = {*GROUPS, "sessions", "conversions"}


def validate_real_model(package):
    if set(package) != {"schema_version", "manifest", "aggregates"} or package["schema_version"] != 2:
        raise ValueError("Invalid real behavior package")
    m, groups = package["manifest"], package["aggregates"]
    if m["source"] != "real" or m["kind"] != "real_behavior" or set(groups) != GROUPS:
        raise ValueError("Real releases cannot include simulated operations")
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", m["run_id"]):
        raise ValueError("Invalid real run ID")
    real_path(m["output"], "warehouse")
    if m["cohort_definition"] != "first_observed_in_retained_30d_window" or m["observation_scope"] != "accepted_events_only":
        raise ValueError("Unbounded cohort claim")
    coverage = validate_coverage(m["coverage"], m["cutoff"])
    if not m.get("input_snapshot") or m["input_snapshot"].get("source") != "real":
        raise ValueError("An immutable real source snapshot is required")
    if not date.fromisoformat(m["date_from"]) <= date.fromisoformat(m["date_to"]):
        raise ValueError("Invalid report window")
    if date.fromisoformat(m["date_to"]) > date.fromisoformat(m["complete_through"]):
        raise ValueError("Unclosed report date")
    if date.fromisoformat(m["complete_through"]) != stamp(m["cutoff"]).astimezone(HK).date() - timedelta(days=1):
        raise ValueError("Closed date does not agree with model cutoff")
    if not stamp(m["cutoff"]) - timedelta(days=30) <= stamp(m["retained_from"]) <= stamp(m["cutoff"]):
        raise ValueError("Observation state exceeds thirty days")
    if set(m["counts"]) != COUNTS or any(type(v) is not int or v < 0 for v in m["counts"].values()):
        raise ValueError("Invalid model row counts")
    quality = m["quality"]
    if any(type(quality.get(k)) is not int or quality[k] < 0 for k in ("raw", "valid", "duplicates", "quarantined", "after_cutoff")):
        raise ValueError("Invalid input quality counters")
    if quality["quarantined"] or quality["raw"] != sum(quality[k] for k in ("valid", "duplicates", "quarantined", "after_cutoff")):
        raise ValueError("Input quality gate failed")
    for name, rows in groups.items():
        if not isinstance(rows, list) or len(rows) > 10000 or len(rows) != m["counts"][name]:
            raise ValueError("Unbounded or mismatched aggregate rows")
        expected = FIELDS[name] | ({"observation_d1", "observation_d7"} if name == "retention" else set())
        seen = set()
        for row in rows:
            if set(row) != expected or row["source"] != "real":
                raise ValueError("Unexpected public/private aggregate fields or source")
            dimensions = {k: v for k, v in row.items() if k in {"source", "app", "date", "cohort_date", "channel"}}
            key = canonical(dimensions)
            if key in seen:
                raise ValueError("Duplicate aggregate grain")
            seen.add(key)
            if any(not isinstance(v, str) or not 1 <= len(v) <= 200 for v in dimensions.values()):
                raise ValueError("Invalid dimension")
            if "app" in row and row["app"] not in {"mywebsite", "project_snow"}:
                raise ValueError("Invalid application")
            day = row.get("date", row.get("cohort_date"))
            date.fromisoformat(day)
            if not m["date_from"] <= day <= m["date_to"]:
                raise ValueError("Row outside declared report dates")
            for field, value in row.items():
                if field in dimensions or field.startswith("observation_") or field.startswith("retained_d") and value is None:
                    continue
                if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
                    raise ValueError("Invalid real aggregate metric")
                if field != "duration_seconds" and type(value) is not int:
                    raise ValueError("Integer aggregate required")
            if name == "retention":
                for lag in (1, 7):
                    expected_state = observation_status(day, lag, m["cutoff"], coverage, m["retained_from"])
                    eligible, retained = row[f"eligible_d{lag}"], row[f"retained_d{lag}"]
                    if row[f"observation_d{lag}"] != expected_state:
                        raise ValueError("Retention coverage status does not match evidence")
                    if expected_state == "complete_accepted_prefix":
                        if eligible != row["users"] or retained is None or retained > eligible:
                            raise ValueError("Invalid observable retention")
                    elif eligible != 0 or retained is not None:
                        raise ValueError("Incomplete or immature retention must stay null")
            if name == "funnel" and not (row["converted"] <= row["requested"] <= row["arrived"] <= row["clicks"] and row["selected"] <= row["arrived"]):
                raise ValueError("Unreconciled attribution")
            if name == "session_daily" and (row["closed_sessions"] > row["sessions"] or row["users"] > row["sessions"]):
                raise ValueError("Unreconciled sessions")
    if (sum(r["sessions"] for r in groups["session_daily"]) != m["counts"]["sessions"] or
            sum(r["converted"] for r in groups["funnel"]) != m["counts"]["conversions"]):
        raise ValueError("Real detail and aggregate counts differ")
    return m


def validate_real_pair(daily, behavior):
    basic = validate(daily)[0]
    modeled = validate_real_model(behavior)
    if basic["source"] != "real":
        raise ValueError("Invalid real release")
    for key in ("input", "input_snapshot", "date_from", "date_to"):
        if basic.get(key) != modeled.get(key):
            raise ValueError("Real models need the same immutable input/window")
    if stamp(basic["cutoff"]) != stamp(modeled["cutoff"]):
        raise ValueError("Real model cutoffs differ")
    return basic, modeled


def real_release(daily, behavior, run_id):
    """Pure aggregate-only release contract; persistence enforces its lifetime."""
    basic, modeled = validate_real_pair(daily, behavior)
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", run_id):
        raise ValueError("Invalid real release ID")
    semantic = dict(source="real", input_snapshot=basic["input_snapshot"], cutoff=stamp(basic["cutoff"]).isoformat(),
                    date_from=basic["date_from"], date_to=basic["date_to"], daily=daily["daily"],
                    aggregates=behavior["aggregates"], coverage=modeled["coverage"], retained_from=modeled["retained_from"])
    result = dict(schema_version=1, source="real", run_id=run_id, daily=daily, behavior=behavior,
                  content_hash=digest(canonical(semantic)),
                  expires_at=(datetime.combine(date.fromisoformat(basic["date_from"]), time(), HK) + timedelta(days=90)).isoformat())
    return json.loads(canonical(result))


def read_real_release(directory, now=None):
    """Startup and every refresh clean expiry before reading any aggregate rows."""
    current = now or datetime.now(UTC)
    directory = Path(directory)
    lifecycle = RealLifecycle(directory / "data")
    lifecycle.cleanup(current)
    pointer = json.loads((directory / "real-latest.json").read_bytes())
    if (set(pointer) != {"schema_version", "source", "file", "sha256", "expires_at"} or
            pointer["schema_version"] != 1 or pointer["source"] != "real" or
            not re.fullmatch(r"releases/[a-f0-9]{64}\.json", pointer["file"])):
        raise ValueError("Invalid real aggregate pointer")
    if stamp(pointer["expires_at"]) <= current:
        raise FileNotFoundError("Real aggregate archive has expired")
    body = lifecycle.readable(pointer["file"], current).read_bytes()
    if digest(body) != pointer["sha256"]:
        raise ValueError("Real aggregate archive checksum differs")
    result = json.loads(body)
    expected = real_release(result["daily"], result["behavior"], result["run_id"])
    if result != expected or result["expires_at"] != pointer["expires_at"]:
        raise ValueError("Real aggregate release identity/expiry differs")
    return result


def release_real(daily, behavior, directory, run_id, *, now=None):
    """Reserve immutable aggregate copy before write; pointer never holds rows."""
    current = now or datetime.now(UTC)
    result = real_release(daily, behavior, run_id)
    basic, modeled = validate_real_pair(daily, behavior)
    if stamp(basic["cutoff"]) > current:
        raise ValueError("Cannot publish a future cutoff")
    origin = datetime.combine(date.fromisoformat(basic["date_from"]), time(), HK)
    directory = Path(directory)
    with publication_lock(directory):
        lifecycle = RealLifecycle(directory / "data")
        if not lifecycle.owner.exists():
            lifecycle.initialize()
        lifecycle.cleanup(current)
        latest = directory / "real-latest.json"
        if latest.exists():
            try:
                old = read_real_release(directory, current)
            except FileNotFoundError:
                old = None
            previous = validate_real_model(old["behavior"]) if old else None
            if previous and (stamp(previous["cutoff"]) > stamp(modeled["cutoff"]) or
                    stamp(previous["cutoff"]) == stamp(modeled["cutoff"]) and old["content_hash"] != result["content_hash"]):
                raise ValueError("Refusing older cutoff or conflicting real release")
        name = "releases/" + digest(canonical(result)) + ".json"
        path = lifecycle.register(name, "aggregate", origin.isoformat(), now=current)
        write_json(path, result)
        lifecycle.cleanup(current)
        write_json(latest, dict(schema_version=1, source="real", file=name, sha256=digest(path.read_bytes()),
                                expires_at=result["expires_at"]))
    return result
