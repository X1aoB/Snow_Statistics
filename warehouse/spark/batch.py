"""Spark 3.5 batch pipeline. Run via spark-submit; dependencies belong to the lab.

Read immutable ODS JSONL envelopes, validate, deduplicate, write a versioned run,
then publish a manifest only after the quality gate. Old runs remain recoverable.
"""
import argparse
import json
from datetime import datetime, timezone

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

parser = argparse.ArgumentParser()
parser.add_argument("--input", required=True)
parser.add_argument("--output", required=True)
parser.add_argument("--run-id", required=True)
parser.add_argument("--date-from", required=True)
parser.add_argument("--date-to", required=True)
parser.add_argument("--cutoff", required=True, help="UTC accepted_at cutoff shared by reconciliation")
args = parser.parse_args()
if not args.run_id.replace("-", "").replace("_", "").isalnum():
    raise ValueError("unsafe run ID")
spark = SparkSession.builder.appName("snow-statistics-batch").enableHiveSupport().getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.shuffle.partitions", "4")
event_schema = StructType([StructField(name, StringType()) for name in
    ("event_id", "schema_version", "app", "event_type", "occurred_at", "anonymous_id", "session_id", "path", "character_id", "jump_id", "channel", "request_id", "success", "elapsed_ms")])
schema = StructType([StructField("seq", LongType()), StructField("source", StringType()),
                     StructField("accepted_at", StringType()), StructField("event", event_schema), StructField("_corrupt_record", StringType())])
raw = spark.read.schema(schema).json(args.input).cache()
base = raw.select("source", "seq", "accepted_at", "_corrupt_record", "event.*").withColumn("event_time", F.to_timestamp("occurred_at"))
valid_condition = (F.col("_corrupt_record").isNull() & F.col("source").isin("real", "synthetic") &
                   F.col("app").isin("mywebsite", "project_snow") & (F.col("schema_version") == "1") &
                   F.col("event_id").isNotNull() & F.col("event_time").isNotNull() &
                   F.col("event_type").isin("page_view", "character_select", "entry_click", "entry_arrival", "request_observed", "request_complete"))
quarantine = base.filter(~F.coalesce(valid_condition, F.lit(False))).select("source", "seq").withColumn("reason", F.lit("invalid_contract"))
valid = base.filter(valid_condition).filter(F.to_timestamp("accepted_at") <= F.to_timestamp(F.lit(args.cutoff)))
excluded = base.filter(valid_condition).filter(F.to_timestamp("accepted_at") > F.to_timestamp(F.lit(args.cutoff))).count()
event_order = Window.partitionBy("source", "app", "event_id").orderBy("accepted_at", "seq")
dedup = valid.withColumn("rn", F.row_number().over(event_order)).filter("rn=1").drop("rn", "_corrupt_record")
dedup = dedup.withColumn("business_key", F.when(F.col("event_type") == "request_complete", F.concat(F.lit("request:"), F.col("request_id"))).otherwise(F.concat(F.lit("event:"), F.col("event_id"))))
request_order = Window.partitionBy("source", "app", "business_key").orderBy("accepted_at", "seq")
dwd_all = dedup.withColumn("rn", F.row_number().over(request_order)).filter("rn=1").drop("rn").cache()
counts = dict(raw=raw.count(), valid=dwd_all.count(), quarantined=quarantine.count(), after_cutoff=excluded)
counts["duplicates"] = counts["raw"] - counts["valid"] - counts["quarantined"] - counts["after_cutoff"]
dwd = dwd_all.withColumn("business_date", F.to_date(F.from_utc_timestamp("event_time", "Asia/Hong_Kong"))).filter(F.col("business_date").between(args.date_from, args.date_to))
daily = dwd.groupBy("source", "app", "business_date").agg(
    F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("pv"),
    F.countDistinct("anonymous_id").alias("uv"),
    F.sum(F.when(F.col("event_type") == "request_complete", 1).otherwise(0)).alias("requests"),
    F.sum(F.when((F.col("event_type") == "request_complete") & (F.col("success") == "true"), 1).otherwise(0)).alias("successes"))
invalid_metrics = daily.filter("pv < 0 OR uv < 0 OR requests < successes OR successes < 0").count()
run = args.output.rstrip("/") + "/runs/" + args.run_id
quarantine.write.mode("errorifexists").parquet(run + "/quarantine")
if counts["quarantined"] or invalid_metrics:
    raise RuntimeError("quality gate failed; previous published manifest remains valid")
dwd.write.mode("errorifexists").partitionBy("source", "business_date").parquet(run + "/dwd")
daily.write.mode("errorifexists").partitionBy("source", "business_date").parquet(run + "/ads_daily")
manifest = dict(schema_version=1, run_id=args.run_id, cutoff=args.cutoff, date_from=args.date_from,
                date_to=args.date_to, output=run, quality=counts, generated_at=datetime.now(timezone.utc).isoformat())
spark.createDataFrame([(json.dumps(manifest),)], ["value"]).coalesce(1).write.mode("errorifexists").text(run + "/accepted")
print(json.dumps(manifest))
spark.stop()
