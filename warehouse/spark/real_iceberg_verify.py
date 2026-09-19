"""Read-only actual current/original snapshot comparison, never a success-JSON import."""
import argparse
import hashlib
import json
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, DoubleType, LongType, StringType, StructField, StructType

parser = argparse.ArgumentParser()
for name in ("input", "input-sha256", "execution", "execution-sha256", "receipt", "read-until"):
    parser.add_argument("--" + name, required=True)
args = parser.parse_args()


def checked(path, expected):
    payload = Path(path).read_bytes()
    if len(payload) > 4 * 1024 * 1024 or hashlib.sha256(payload).hexdigest() != expected:
        raise ValueError("Verification input changed")
    return json.loads(payload)


bundle = checked(args.input, args.input_sha256)
execution = checked(args.execution, args.execution_sha256)
deadline = min(datetime.fromisoformat(args.read_until), datetime.fromisoformat(bundle["expires_at"]))
if datetime.now(timezone.utc) >= deadline:
    raise ValueError("Read-only verification admission expired")
if bundle["source"] != "real" or set(bundle["aggregates"]) != {"daily", "session_daily", "retention", "funnel"}:
    raise ValueError("Only the accepted real aggregate bundle may be verified")
spark = SparkSession.builder.appName("snow-real-iceberg-verify-" + bundle["run_id"]).getOrCreate()
try:
    if spark.sparkContext.master != "yarn" or spark.conf.get("spark.sql.catalog.real_lake.warehouse") != bundle["warehouse"]:
        raise ValueError("Verification engine/catalog differs")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    types = dict(string=StringType(), date=DateType(), long=LongType(), double=DoubleType())
    tables = {}
    for name, rows in bundle["aggregates"].items():
        if datetime.now(timezone.utc) >= deadline:
            raise ValueError("Read-only verification admission expired")
        fields = bundle["columns"][name]
        values = [tuple(date.fromisoformat(row[field]) if kind == "date" and row[field] is not None else
                        float(row[field]) if kind == "double" and row[field] is not None else row[field]
                        for field, kind in fields.items()) for row in rows]
        frame = spark.createDataFrame(values, StructType([StructField(field, types[kind], True) for field, kind in fields.items()]))
        table = "real_lake.analytics." + name
        expected = execution["tables"][name]
        actual = spark.table(table)
        if set(actual.columns) != set(fields) | {"model_revision"}:
            raise ValueError("Actual Iceberg schema differs")
        if (any(actual.schema[field].dataType != types[kind] for field, kind in fields.items())
                or actual.schema["model_revision"].dataType != StringType()):
            raise ValueError("Actual Iceberg column types differ")

        def same(actual):
            actual = actual.select(frame.columns)
            if frame.exceptAll(actual).take(1) or actual.exceptAll(frame).take(1):
                raise ValueError("Actual Iceberg snapshot differs from the issued aggregate pair")

        same(actual)
        history = spark.sql("SELECT snapshot_id FROM " + table + ".history ORDER BY made_current_at DESC LIMIT 1").first()
        current = history[0] if history else None
        original = expected["original_snapshot"]
        if current != expected["current_snapshot"]:
            raise ValueError("Actual current snapshot changed after execution")
        if original is not None:
            if type(original) is not int:
                raise ValueError("Snapshot identifier must be an integer")
            same(spark.sql("SELECT * FROM " + table + " VERSION AS OF " + str(original)))
        elif rows or current is not None:
            raise ValueError("Only an empty table can lack the original snapshot")
        location = spark.sql("DESCRIBE TABLE EXTENDED " + table).filter("col_name = 'Location'").first()
        if not location or location[1].rstrip("/") != bundle["warehouse"] + "/analytics/" + name:
            raise ValueError("Actual Iceberg location escaped the registered scope")
        tables[name] = dict(name=table, rows=len(rows), original_snapshot=original, current_snapshot=current,
                            exact_readback_equal=True, historical_readback_equal=True if original is not None else None,
                            location=location[1].rstrip("/"), additive_column="model_revision")
    if datetime.now(timezone.utc) >= deadline:
        raise ValueError("Verification completed after the admission deadline")
    receipt = dict(schema_version=1, source="real", run_id=bundle["run_id"], input_sha256=args.input_sha256,
                   warehouse=bundle["warehouse"], expires_at=bundle["expires_at"], engine="Spark " + spark.version,
                   master=spark.sparkContext.master, application_id=spark.sparkContext.applicationId,
                   hive_registration=False, column_lineage=False, tables=tables)
    target = Path(args.receipt)
    temporary = target.with_suffix(".tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(receipt, stream, sort_keys=True)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, target)
finally:
    spark.stop()
sys.exit(0)
