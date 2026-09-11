"""Migrate accepted synthetic DWD into an isolated HDFS Iceberg catalog."""
import argparse
import json
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--attempt", required=True)
parser.add_argument("--verify", action="store_true")
args = parser.parse_args()
if not args.attempt.replace("-", "").isalnum():
    raise ValueError("Unsafe attempt")
out = Path("/opt/snow/runtime/lake") / args.attempt
receipt = out / "accepted.json"
if not args.verify:
    out.mkdir(parents=True, exist_ok=False)
elif not receipt.is_file() or (out / "restored.json").exists():
    raise ValueError("A preserved accepted table and new restore receipt are required")
spark = SparkSession.builder.appName("Snow Statistics synthetic Iceberg DWD " + args.attempt).getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.shuffle.partitions", "4")
table = "snow.synthetic.dwd"


def save(name, value):
    tmp = out / (name + ".tmp")
    with tmp.open("w", encoding="utf-8") as stream:
        json.dump(value, stream, sort_keys=True, indent=2, default=str)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(tmp, out / name)


def snapshot():
    return spark.sql("SELECT snapshot_id FROM " + table + ".history ORDER BY made_current_at DESC LIMIT 1").first()[0]


def same(left, right):
    right = right.select(left.columns)
    assert not left.exceptAll(right).take(1)
    assert not right.exceptAll(left).take(1)


try:
    assert spark.sparkContext.master == "yarn"
    original = spark.read.parquet("/snow/warehouse/scale/runs/scale-100k-01/dwd")
    assert original.count() == 90000 and original.filter("source <> 'synthetic'").count() == 0
    if args.verify:
        accepted = json.loads(receipt.read_bytes())
        assert spark.table(table).count() == accepted["final_rows"] == 90001
        assert snapshot() == accepted["final_snapshot"]
        old = spark.sql("SELECT * FROM " + table + " VERSION AS OF " + str(accepted["original_snapshot"]))
        same(original, old)
        assert spark.sql("SELECT count(*) FROM " + table + " WHERE classification_revision='v2'").first()[0] == 1
        save("restored.json", dict(source="synthetic", master="yarn", application_id=spark.sparkContext.applicationId,
                                    final_snapshot=snapshot(), final_rows=90001, original_snapshot_rows=90000,
                                    exact_original_equal=True, catalog_reopened=True))
    else:
        original.createOrReplaceTempView("source_dwd")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS snow.synthetic")
        spark.sql("CREATE TABLE " + table + " USING iceberg PARTITIONED BY (months(business_date)) "
                  "TBLPROPERTIES('format-version'='2') AS SELECT * FROM source_dwd")
        same(original, spark.table(table))
        first = snapshot()
        spark.sql("ALTER TABLE " + table + " ADD COLUMN classification_revision STRING")
        original.filter("seq=1").withColumn("classification_revision", F.lit("v2")).createOrReplaceTempView("correction")
        spark.sql("MERGE INTO " + table + " t USING correction s ON t.event_id=s.event_id "
                  "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
        assert spark.table(table).count() == 90000
        assert spark.sql("SELECT count(*) FROM " + table + " WHERE classification_revision='v2'").first()[0] == 1
        spark.sql("ALTER TABLE " + table + " REPLACE PARTITION FIELD months(business_date) WITH days(business_date)")
        for number in (1, 2):
            event_id = "ffffffff-ffff-4fff-8fff-" + str(number).zfill(12)
            row = original.filter("seq=1").withColumn("seq", F.lit(1000000 + number).cast("long"))
            row = (row.withColumn("event_id", F.lit(event_id))
                   .withColumn("business_key", F.lit("event:" + event_id))
                   .withColumn("occurred_at", F.lit("2026-02-01T00:00:00Z"))
                   .withColumn("accepted_at", F.lit("2026-02-01T00:00:01Z"))
                   .withColumn("event_time", F.to_timestamp(F.lit("2026-02-01T00:00:00Z")))
                   .withColumn("business_date", F.to_date(F.lit("2026-02-01")))
                   .withColumn("classification_revision", F.lit("v3")))
            row.writeTo(table).append()
        assert spark.table(table).count() == 90002
        spark.sql("DELETE FROM " + table + " WHERE seq=1000002")
        assert spark.table(table).count() == 90001
        before_rewrite = snapshot()
        rewritten = [r.asDict() for r in spark.sql(
            "CALL snow.system.rewrite_data_files(table => 'synthetic.dwd', "
            "options => map('rewrite-all','true','target-file-size-bytes','134217728'))").collect()]
        assert sum(r["rewritten_data_files_count"] for r in rewritten) > 0
        same(spark.sql("SELECT * FROM " + table + " VERSION AS OF " + str(before_rewrite)), spark.table(table))
        same(original, spark.sql("SELECT * FROM " + table + " VERSION AS OF " + str(first)))
        snapshots = [r.asDict() for r in spark.sql("SELECT snapshot_id,parent_id,operation,summary FROM " + table + ".snapshots").collect()]
        files = [r.asDict() for r in spark.sql("SELECT spec_id,count(*) files,sum(record_count) records FROM " + table + ".files GROUP BY spec_id").collect()]
        save("accepted.json", dict(schema_version=1, source="synthetic", iceberg="1.10.0", spark=spark.version,
                                    master="yarn", application_id=spark.sparkContext.applicationId,
                                    input="/snow/warehouse/scale/runs/scale-100k-01/dwd", table=table,
                                    catalog=spark.conf.get("spark.sql.catalog.snow.warehouse"),
                                    original_rows=90000, original_snapshot=first, final_rows=90001,
                                    final_snapshot=snapshot(), before_rewrite_snapshot=before_rewrite,
                                    exact_migration_equal=True, exact_original_time_travel_equal=True,
                                    exact_rewrite_equal=True, merge=True, delete=True, schema_evolution=True,
                                    partition_evolution=True, rewritten=rewritten, current_files=files, snapshots=snapshots,
                                    retention="All snapshots and source Parquet preserved; no orphan/snapshot deletion"))
    print(json.dumps(dict(attempt=args.attempt, verify=args.verify, rows=90001, snapshot=snapshot())), flush=True)
finally:
    spark.stop()
