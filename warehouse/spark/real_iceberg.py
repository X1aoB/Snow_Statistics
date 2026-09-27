"""Actual Iceberg storage/readback for previously validated real aggregates only."""
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
parser.add_argument("--input", required=True)
parser.add_argument("--input-sha256", required=True)
parser.add_argument("--receipt", required=True)
args = parser.parse_args()
blob = Path(args.input).read_bytes()
if len(blob) > 4 * 1024 * 1024 or hashlib.sha256(blob).hexdigest() != args.input_sha256:
    raise ValueError("Only a bounded validated immutable input bundle may be read")
bundle = json.loads(blob)
if (bundle["schema_version"] != 1 or bundle["source"] != "real" or bundle["kind"] != "real_aggregate_lake_input" or
        datetime.fromisoformat(bundle["expires_at"]) <= datetime.now(timezone.utc)):
    raise ValueError("Invalid or expired real aggregate input")
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
# This driver is Python 3.8-compatible; the JSON schema is fixed below rather
# than importing the Python 3.12 publication service inside the Spark image.
types = dict(string=StringType(), date=DateType(), long=LongType(), double=DoubleType())
forbidden = {"anonymous_id", "event_id", "request_id", "jump_id", "payload", "path", "character_id", "elapsed_ms"}
if set(bundle["aggregates"]) != {"daily", "session_daily", "retention", "funnel"}:
    raise ValueError("Only the four accepted aggregate groups are supported")
if any(forbidden & set(fields) for fields in bundle["columns"].values()):
    raise ValueError("Row-level fields must never enter this real lake job")
spark = SparkSession.builder.appName("snow-real-aggregate-iceberg-" + bundle["run_id"]).getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.shuffle.partitions", "4")
if spark.sparkContext.master != "yarn" or spark.conf.get("spark.sql.catalog.real_lake.warehouse") != bundle["warehouse"]:
    raise ValueError("Unexpected actual execution engine or catalog location")
spark.sql("CREATE NAMESPACE IF NOT EXISTS real_lake.analytics")
tables = {}
for name, rows in bundle["aggregates"].items():
    fields = bundle["columns"][name]
    schema = StructType([StructField(field, types[kind], True) for field, kind in fields.items()])
    values = []
    for row in rows:
        if set(row) != set(fields) or row["source"] != "real":
            raise ValueError("Mixed source or aggregate field drift")
        values.append(tuple(date.fromisoformat(row[field]) if kind == "date" and row[field] is not None else
                            float(row[field]) if kind == "double" and row[field] is not None else row[field]
                            for field, kind in fields.items()))
    frame = spark.createDataFrame(values, schema)
    frame.createOrReplaceTempView("accepted_" + name)
    table = "real_lake.analytics." + name
    partition = "cohort_date" if name == "retention" else "date"
    spark.sql("CREATE TABLE " + table + " USING iceberg PARTITIONED BY (days(" + partition + ")) "
              "TBLPROPERTIES('format-version'='2') AS SELECT * FROM accepted_" + name)
    def same(actual):
        actual = actual.select(frame.columns)
        if frame.exceptAll(actual).take(1) or actual.exceptAll(frame).take(1):
            raise ValueError("Materialized real Iceberg rows differ from accepted aggregates")
    same(spark.table(table))
    history = spark.sql("SELECT snapshot_id FROM " + table + ".history ORDER BY made_current_at DESC LIMIT 1").first()
    original = history[0] if history else None
    # Metadata-only additive evolution does not invent usage values or alter
    # the original snapshot. Even empty groups have a real table and snapshot.
    spark.sql("ALTER TABLE " + table + " ADD COLUMN model_revision STRING")
    if original is not None:
        same(spark.sql("SELECT * FROM " + table + " VERSION AS OF " + str(original)))
    same(spark.table(table))
    history = spark.sql("SELECT snapshot_id FROM " + table + ".history ORDER BY made_current_at DESC LIMIT 1").first()
    current = history[0] if history else None
    tables[name] = dict(name=table, rows=len(rows), original_snapshot=original, current_snapshot=current,
                        exact_readback_equal=True, historical_readback_equal=True if original is not None else None,
                        location=bundle["warehouse"] + "/analytics/" + name, additive_column="model_revision")
receipt = dict(schema_version=1, source="real", run_id=bundle["run_id"], input_sha256=args.input_sha256,
               warehouse=bundle["warehouse"], expires_at=bundle["expires_at"], engine="Spark " + spark.version,
               master=spark.sparkContext.master, application_id=spark.sparkContext.applicationId,
               hive_registration=False, column_lineage=False, tables=tables)
target = Path(args.receipt)
target.parent.mkdir(parents=True, exist_ok=True)
temporary = target.with_suffix(".tmp")
with temporary.open("w", encoding="utf-8") as stream:
    json.dump(receipt, stream, sort_keys=True)
    stream.flush()
    os.fsync(stream.fileno())
os.replace(temporary, target)
print(json.dumps(receipt, sort_keys=True))
spark.stop()
