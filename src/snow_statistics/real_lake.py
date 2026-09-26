"""Only validated real aggregate pairs may be copied into the Iceberg experiment."""
import re
from datetime import UTC, datetime, time, timedelta

from .io import digest
from .publication import canonical
from .real_behavior import HK, real_path, stamp
from .real_publication import validate_real_pair

LAKE_COLUMNS = {
    "daily": {"source": "string", "app": "string", "date": "date", "pv": "long", "uv": "long", "requests": "long", "successes": "long"},
    "session_daily": {"source": "string", "app": "string", "date": "date", "sessions": "long", "users": "long", "events": "long", "duration_seconds": "double", "closed_sessions": "long"},
    "retention": {"source": "string", "app": "string", "cohort_date": "date", "users": "long", "eligible_d1": "long", "retained_d1": "long", "eligible_d7": "long", "retained_d7": "long", "observation_d1": "string", "observation_d7": "string"},
    "funnel": {"source": "string", "channel": "string", "date": "date", "clicks": "long", "arrived": "long", "selected": "long", "requested": "long", "converted": "long"},
}


def prepare_bundle(daily, behavior, run_id, warehouse, *, now=None):
    manifest, modeled = validate_real_pair(daily, behavior)
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", run_id):
        raise ValueError("Unsafe real lake run ID")
    real_path(warehouse, "warehouse")
    if "/iceberg/" not in warehouse:
        raise ValueError("Use a dedicated real Iceberg output subtree")
    origin = datetime.combine(datetime.fromisoformat(manifest["date_from"]).date(), time(), HK)
    expires = origin + timedelta(days=90)
    if expires <= (now or datetime.now(UTC)):
        raise ValueError("Source aggregates expired; copying cannot renew them")
    rows = {"daily": daily["daily"], **behavior["aggregates"]}
    for name, values in rows.items():
        if any(set(row) != set(LAKE_COLUMNS[name]) or row["source"] != "real" for row in values):
            raise ValueError("Only exact aggregate fields may enter real Iceberg")
    return dict(schema_version=1, source="real", kind="real_aggregate_lake_input", run_id=run_id,
                warehouse=warehouse, cutoff=manifest["cutoff"], date_from=manifest["date_from"], date_to=manifest["date_to"],
                original_at=origin.isoformat(), expires_at=expires.isoformat(),
                validated_pair_sha256=digest(canonical({"daily": daily, "behavior": behavior})),
                input_snapshot=manifest["input_snapshot"], coverage=modeled["coverage"],
                columns=LAKE_COLUMNS, aggregates=rows)


def validate_receipt(receipt, bundle, *, now=None):
    if (receipt.get("schema_version") != 1 or receipt.get("source") != "real" or
            receipt.get("run_id") != bundle["run_id"] or receipt.get("warehouse") != bundle["warehouse"] or
            receipt.get("input_sha256") != digest(canonical(bundle)) or receipt.get("expires_at") != bundle["expires_at"] or
            receipt.get("engine") != "Spark 3.5.7" or receipt.get("master") != "yarn" or
            not re.fullmatch(r"application_[0-9]+_[0-9]+", receipt.get("application_id", ""))):
        raise ValueError("Iceberg receipt is not bound to the validated real aggregate input")
    if stamp(receipt["expires_at"]) <= (now or datetime.now(UTC)):
        raise ValueError("Real lake receipt expired")
    if set(receipt.get("tables", {})) != set(LAKE_COLUMNS):
        raise ValueError("Missing real aggregate tables")
    for name, table in receipt["tables"].items():
        if (table.get("name") != "real_lake.analytics." + name or table.get("rows") != len(bundle["aggregates"][name]) or
                table.get("exact_readback_equal") is not True or
                table.get("location") != bundle["warehouse"] + "/analytics/" + name):
            raise ValueError("Iceberg table evidence differs from the validated aggregates")
        if table.get("original_snapshot") is None:
            if table["rows"] != 0 or table.get("historical_readback_equal") is not None or table.get("current_snapshot") is not None:
                raise ValueError("Only an empty table may honestly have no snapshot")
        elif table.get("historical_readback_equal") is not True or table.get("current_snapshot") is None:
            raise ValueError("Existing snapshots require actual historical readback")
    if receipt.get("hive_registration") is not False or receipt.get("column_lineage") is not False:
        raise ValueError("Do not declare unperformed Hive or column-lineage work")
    return receipt
