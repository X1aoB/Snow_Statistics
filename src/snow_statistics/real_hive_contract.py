"""Python 3.8-compatible allowlists for the locked Spark catalog process."""
import hashlib
import ipaddress
import json
import re
from datetime import datetime, timedelta, timezone
from uuid import UUID

GROUP_COLUMNS = {
    "daily": {"source", "app", "date", "pv", "uv", "requests", "successes"},
    "session_daily": {"source", "app", "date", "sessions", "users", "events", "duration_seconds", "closed_sessions"},
    "retention": {"source", "app", "cohort_date", "users", "eligible_d1", "retained_d1", "eligible_d7", "retained_d7", "observation_d1", "observation_d7"},
    "funnel": {"source", "channel", "date", "clicks", "arrived", "selected", "requested", "converted"},
}


def stamp(value):
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("Original Hive deadlines must have timezone")
    return result.astimezone(timezone.utc)


def canonical(value):
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def rows_hash(rows):
    return hashlib.sha256(canonical(sorted(rows, key=canonical))).hexdigest()


def validate_descriptor(table, value):
    required = {"group", "location", "owner_sha256", "collector", "release_sha256", "pair_sha256", "original_at", "expires_at", "expected_rows", "expected_rows_sha256"}
    if set(value) != required or value["group"] not in GROUP_COLUMNS:
        raise ValueError("Unknown Hive descriptor fields or aggregate group")
    for key in ("owner_sha256", "release_sha256", "pair_sha256", "expected_rows_sha256"):
        if not re.fullmatch(r"[a-f0-9]{64}", value[key]):
            raise ValueError("Invalid Hive immutable checksum")
    expected = "snow_real.catalog_" + value["owner_sha256"][:32] + "_" + value["release_sha256"][:32] + "_" + value["group"]
    if table != expected:
        raise ValueError("Hive table must be the exact generation/release-bound identifier")
    if (not re.fullmatch(r"hdfs://[A-Za-z0-9.-]+:9000/snow/warehouse/real/[A-Za-z0-9_/-]{1,200}", value["location"]) or
            any(part in {".", "..", ""} for part in value["location"].split(":9000/", 1)[-1].split("/"))):
        raise ValueError("Invalid real external table location")
    expected_leaf = "ads_daily" if value["group"] == "daily" else value["group"]
    if not value["location"].endswith("/" + expected_leaf):
        raise ValueError("Hive group does not match its original materialized location")
    collector = value["collector"]
    if set(collector) != {"schema_version", "source", "instance_id", "generation"} or type(collector["schema_version"]) is not int or collector["schema_version"] != 1 or collector["source"] != "real":
        raise ValueError("Hive requires the real collector identity")
    for key in ("instance_id", "generation"):
        if str(UUID(collector[key])) != collector[key]:
            raise ValueError("Invalid canonical collector UUID")
    if stamp(value["expires_at"]) != stamp(value["original_at"]) + timedelta(days=90):
        raise ValueError("A Hive table cannot renew its original aggregate expiry")
    if type(value["expected_rows"]) is not int or not 0 <= value["expected_rows"] <= 10000:
        raise ValueError("Unbounded aggregate row count")
    return value


def properties(value):
    return {"snow.project": "snow-statistics", "snow.source": "real", "snow.owner_sha256": value["owner_sha256"],
            "snow.collector_instance": value["collector"]["instance_id"], "snow.collector_generation": value["collector"]["generation"],
            "snow.release_sha256": value["release_sha256"], "snow.pair_sha256": value["pair_sha256"],
            "snow.original_at": value["original_at"], "snow.expires_at": value["expires_at"], "external.table.purge": "false"}


def validate_metadata(value, metadata):
    expected_columns = GROUP_COLUMNS[value["group"]]
    if value["group"] == "daily":
        expected_columns = expected_columns - {"date"} | {"business_date"}
    if (metadata["type"] != "EXTERNAL" or metadata["provider"].lower() != "parquet" or
            metadata["location"] != value["location"] or metadata["properties"] != properties(value) or
            set(metadata["columns"]) != expected_columns or len(metadata["columns"]) != len(expected_columns) or
            metadata["partition_columns"] != (["source", "business_date"] if value["group"] == "daily" else [])):
        raise ValueError("Hive external type, location or ownership properties changed")
    if metadata["partitions"] and value["group"] != "daily":
        raise ValueError("Unexpected partitions on an unpartitioned aggregate")
    for partition, location in metadata["partitions"].items():
        if not re.fullmatch(r"source=real/business_date=\d{4}-\d{2}-\d{2}", partition) or location != value["location"] + "/" + partition:
            raise ValueError("Hive partition escaped its real source or exact registered location")
        day = datetime.fromisoformat(partition.split("=")[-1]).date()
        zone = timezone(timedelta(hours=8))
        if not stamp(value["original_at"]).astimezone(zone).date() <= day < stamp(value["expires_at"]).astimezone(zone).date():
            raise ValueError("Hive partition date is outside its original aggregate lifetime")
    return metadata


def validate_request(request, *, now):
    if (set(request) != {"schema_version", "action", "metastore_uri", "owner_sha256", "tables", "known_tables", "requested_at"} or
            type(request["schema_version"]) is not int or request["schema_version"] != 1 or request["action"] not in {"cleanup", "register", "verify"} or
            not re.fullmatch(r"thrift://[0-9.]+:9083", request["metastore_uri"]) or
            not re.fullmatch(r"[a-f0-9]{64}", request["owner_sha256"]) or
            not isinstance(request["tables"], dict) or len(request["tables"]) > 4000 or
            not isinstance(request["known_tables"], list) or len(request["known_tables"]) > 4000):
        raise ValueError("Unexpected or unbounded catalog request")
    if not stamp(request["requested_at"]) <= now < stamp(request["requested_at"]) + timedelta(minutes=10):
        raise ValueError("Catalog request is stale or from the future")
    address = ipaddress.ip_address(request["metastore_uri"][len("thrift://"):-len(":9083")])
    if address.version != 4 or not address.is_private or address.is_loopback:
        raise ValueError("Metastore must use the configured private lab address")
    prefix = "snow_real.catalog_" + request["owner_sha256"][:32] + "_"
    if (len(set(request["known_tables"])) != len(request["known_tables"]) or
            not set(request["tables"]) <= set(request["known_tables"]) or
            any(not re.fullmatch(re.escape(prefix) + r"[a-f0-9]{32}_(?:daily|session_daily|retention|funnel)", name) for name in request["known_tables"])):
        raise ValueError("Invalid catalog ownership inventory")
    if request["action"] == "cleanup" and set(request["tables"]) != set(request["known_tables"]):
        raise ValueError("Cleanup cannot omit registered tables")
    for name, entry in request["tables"].items():
        if set(entry) != {"descriptor", "verified"} or type(entry["verified"]) is not bool:
            raise ValueError("Invalid catalog registration state")
        validate_descriptor(name, entry["descriptor"])
        if entry["descriptor"]["owner_sha256"] != request["owner_sha256"]:
            raise ValueError("Catalog descriptor belongs to another owner")
        if request["action"] != "cleanup" and stamp(entry["descriptor"]["expires_at"]) <= now:
            raise ValueError("Expired aggregate cannot be registered or read")
    return prefix


def operate(request, catalog, *, clock=lambda: datetime.now(timezone.utc)):
    """Pure orchestration tested with synthetic adapters; real adapter uses Spark."""
    prefix = validate_request(request, now=clock())
    if set(catalog.names(prefix)) - set(request["known_tables"]):
        raise ValueError("Unregistered owned Hive table blocks catalog operations")
    tables, checked = request["tables"], {}
    # Validate ALL selected metadata before any row read or deletion.
    for table, entry in tables.items():
        meta = catalog.inspect(table)
        if meta is not None:
            validate_metadata(entry["descriptor"], meta)
        checked[table] = meta
    result = {}
    for table, entry in tables.items():
        descriptor, meta = entry["descriptor"], checked[table]
        validate_request(request, now=clock())
        expired = stamp(descriptor["expires_at"]) <= clock()
        if request["action"] == "cleanup":
            if expired and meta is not None:
                validate_metadata(descriptor, catalog.inspect(table))
                catalog.drop(table)
                if catalog.inspect(table) is not None:
                    raise ValueError("Expired Hive table survived actual DROP readback")
                meta = None
            if not expired and entry["verified"] and meta is None:
                raise ValueError("Previously verified live Hive table disappeared")
            result[table] = {"state": "absent" if meta is None else "present"}
            continue
        if meta is None:
            if request["action"] != "register":
                raise ValueError("Verification cannot create a missing Hive table")
            catalog.create(table, descriptor)
        validate_metadata(descriptor, catalog.inspect(table))
        if stamp(descriptor["expires_at"]) <= clock():
            raise ValueError("Aggregate expired before Hive row read")
        rows = catalog.rows(table, descriptor)
        if (len(rows) != descriptor["expected_rows"] or any(set(row) != GROUP_COLUMNS[descriptor["group"]] or row["source"] != "real" for row in rows)
                or rows_hash(rows) != descriptor["expected_rows_sha256"]):
            raise ValueError("Hive aggregate values differ from the accepted release")
        if stamp(descriptor["expires_at"]) <= clock():
            raise ValueError("Aggregate expired during Hive row read")
        result[table] = {"state": "present", "rows": len(rows), "rows_sha256": rows_hash(rows)}
    return result
