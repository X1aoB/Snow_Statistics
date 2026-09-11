"""Validate private model aggregates and atomically release a matching model pair."""
import math
import re
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from .io import digest, write_json
from .publication import canonical, publication_lock
from .scheduling import resolve_window

FIELDS = {
    "session_daily": {"source", "app", "date", "sessions", "users", "events", "duration_seconds", "closed_sessions"},
    "retention": {"source", "app", "cohort_date", "users", "eligible_d1", "retained_d1", "eligible_d7", "retained_d7"},
    "funnel": {"source", "channel", "date", "clicks", "arrived", "selected", "requested", "converted"},
    "ticket_daily": {"source", "date", "status", "tickets"},
    "current_categories": {"source", "category", "contents"},
}
COUNTS = {"operations": {"dim_content_scd2", "fact_ticket_round", "fact_ticket_daily"},
          "behavior": {"sessions", "session_daily", "retention", "conversions", "funnel"}}


def validate_model(package, kind=None):
    if set(package) != {"schema_version", "manifest", "aggregates"} or package["schema_version"] != 1:
        raise ValueError("Invalid private model package")
    m, groups = package["manifest"], package["aggregates"]
    if m["kind"] not in COUNTS or kind and m["kind"] != kind or m["source"] != "synthetic":
        raise ValueError("Unexpected model/source")
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", m["run_id"]):
        raise ValueError("Invalid model run ID")
    resolve_window(m["cutoff"], m["run_id"], m["date_from"], m["date_to"], m["cutoff"])
    if set(m["counts"]) != COUNTS[m["kind"]] or any(type(v) is not int or v < 0 for v in m["counts"].values()):
        raise ValueError("Invalid model counts")
    expected = {"session_daily", "retention", "funnel"} if m["kind"] == "behavior" else {"ticket_daily", "current_categories"}
    if set(groups) != expected:
        raise ValueError("Unexpected aggregate tables")
    for name, rows in groups.items():
        if not isinstance(rows, list) or len(rows) > 10000:
            raise ValueError("Unbounded aggregate package")
        seen = set()
        for row in rows:
            if set(row) != FIELDS[name] or row["source"] != "synthetic":
                raise ValueError("Unexpected aggregate fields/source")
            dimensions = {k: v for k, v in row.items() if k in {"source", "app", "date", "cohort_date", "channel", "status", "category"}}
            key = canonical(dimensions)
            if key in seen:
                raise ValueError("Duplicate aggregate grain")
            seen.add(key)
            if "app" in row and row["app"] not in ("mywebsite", "project_snow"):
                raise ValueError("Invalid application")
            day = row.get("date", row.get("cohort_date"))
            if day is not None:
                date.fromisoformat(day)
                if not m["date_from"] <= day <= m["date_to"]:
                    raise ValueError("Aggregate outside report window")
            if any(not isinstance(v, str) or not 1 <= len(v) <= 200 for v in dimensions.values()):
                raise ValueError("Invalid aggregate dimension")
            for field, value in row.items():
                if field in dimensions or field.startswith("retained_d") and value is None:
                    continue
                if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
                    raise ValueError("Invalid aggregate metric")
                if field != "duration_seconds" and type(value) is not int:
                    raise ValueError("Integer metric required")
            if name == "retention":
                for lag in (1, 7):
                    eligible, retained = row[f"eligible_d{lag}"], row[f"retained_d{lag}"]
                    if eligible not in (0, row["users"]) or (eligible == 0) != (retained is None) or retained is not None and retained > eligible:
                        raise ValueError("Invalid retention maturity/denominator")
            if name == "funnel" and not (row["converted"] <= row["requested"] <= row["arrived"] <= row["clicks"] and row["selected"] <= row["arrived"]):
                raise ValueError("Invalid funnel counts")
            if name == "session_daily" and (row["closed_sessions"] > row["sessions"] or row["users"] > row["sessions"]):
                raise ValueError("Invalid session counts")
    if m["kind"] == "behavior":
        if any(len(groups[k]) != m["counts"][k] for k in expected):
            raise ValueError("Aggregate row counts differ from manifest")
        if sum(r["sessions"] for r in groups["session_daily"]) != m["counts"]["sessions"] or sum(r["converted"] for r in groups["funnel"]) != m["counts"]["conversions"]:
            raise ValueError("Detail/aggregate reconciliation failed")
        q = m["quality"]
        if any(type(q[k]) is not int or q[k] < 0 for k in ("raw", "valid", "duplicates", "quarantined", "after_cutoff")) or q["quarantined"] != 0 or q["raw"] != sum(q[k] for k in ("valid", "duplicates", "quarantined", "after_cutoff")):
            raise ValueError("Behavior input quality gate failed")
        cutoff = datetime.fromisoformat(m["cutoff"].replace("Z", "+00:00"))
        through = (cutoff.astimezone(UTC) + timedelta(hours=8)).date() - timedelta(days=1)
        if date.fromisoformat(m["date_to"]) > through:
            raise ValueError("Behavior report dates must be closed at cutoff")
        for row in groups["retention"]:
            for lag in (1, 7):
                mature = date.fromisoformat(row["cohort_date"]) + timedelta(days=lag) <= through
                if (row[f"retained_d{lag}"] is not None) != mature:
                    raise ValueError("Retention maturity disagrees with cutoff")
    elif sum(r["tickets"] for r in groups["ticket_daily"]) != m["counts"]["fact_ticket_daily"]:
        raise ValueError("Ticket snapshot reconciliation failed")
    return m


def release_models(operations, behavior, directory, run_id):
    ops, active = validate_model(operations, "operations"), validate_model(behavior, "behavior")
    context = ("source", "input", "date_from", "date_to")
    if any(ops[k] != active[k] for k in context) or datetime.fromisoformat(ops["cutoff"].replace("Z", "+00:00")) != datetime.fromisoformat(active["cutoff"].replace("Z", "+00:00")):
        raise ValueError("Models do not share the same frozen input/window")
    if not ops.get("input_snapshot") or ops["input_snapshot"] != active.get("input_snapshot"):
        raise ValueError("Model release requires the same immutable ODS snapshot")
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", run_id):
        raise ValueError("Invalid release ID")
    result = dict(schema_version=1, run_id=run_id, operations=operations, behavior=behavior)
    semantic = {"input": ops["input"], "cutoff": datetime.fromisoformat(ops["cutoff"].replace("Z", "+00:00")).astimezone(UTC).isoformat(), "date_from": ops["date_from"], "date_to": ops["date_to"],
                "operations": {"counts": ops["counts"], "aggregates": operations["aggregates"]},
                "behavior": {"counts": active["counts"], "aggregates": behavior["aggregates"]}}
    result["content_hash"] = digest(canonical(semantic))
    directory = Path(directory)
    with publication_lock(directory):
        latest = directory / "models-latest.json"
        if latest.exists():
            import json
            old = json.loads(latest.read_bytes())
            previous = validate_model(old["operations"], "operations")
            old_cutoff = datetime.fromisoformat(previous["cutoff"].replace("Z", "+00:00"))
            cutoff = datetime.fromisoformat(ops["cutoff"].replace("Z", "+00:00"))
            if old_cutoff > cutoff or old_cutoff == cutoff and old["content_hash"] != result["content_hash"]:
                raise ValueError("Refusing older cutoff or conflicting model release")
        write_json(directory / "model-releases" / (digest(canonical(result)) + ".json"), result)
        write_json(latest, result)
    return result
