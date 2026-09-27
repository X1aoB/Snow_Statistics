"""Shared ODS validation and business deduplication for daily and behavior models."""
from ods_input import spark_inputs
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType


def read_events(spark, path, source, cutoff):
    event_schema = StructType([StructField(name, StringType()) for name in
        ("event_id", "schema_version", "app", "event_type", "occurred_at", "anonymous_id", "session_id", "path", "character_id", "jump_id", "channel", "request_id", "success", "elapsed_ms")])
    schema = StructType([StructField("seq", LongType()), StructField("source", StringType()),
                         StructField("accepted_at", StringType()), StructField("event", event_schema), StructField("_corrupt_record", StringType())])
    paths, snapshot = spark_inputs(spark, path, "events", source)
    if snapshot and source != snapshot["source"]:
        raise ValueError("Snapshot source differs from the requested model")
    raw = spark.read.schema(schema).json(paths).cache()
    base = raw.select("source", "seq", "accepted_at", "_corrupt_record", "event.*").withColumn("event_time", F.to_timestamp("occurred_at"))
    condition = (F.col("_corrupt_record").isNull() & (F.col("source") == source) &
                 F.to_timestamp("accepted_at").isNotNull() & (F.col("seq") > 0) &
                 F.col("app").isin("mywebsite", "project_snow") & (F.col("schema_version") == "1") &
                 F.col("event_id").isNotNull() & F.col("event_time").isNotNull() &
                 F.col("event_type").isin("page_view", "character_select", "entry_click", "entry_arrival", "request_observed", "request_complete"))
    quarantine = base.filter(~F.coalesce(condition, F.lit(False))).select("source", "seq").withColumn("reason", F.lit("invalid_contract"))
    valid = base.filter(condition).filter(F.to_timestamp("accepted_at") <= F.to_timestamp(F.lit(cutoff)))
    excluded = base.filter(condition).filter(F.to_timestamp("accepted_at") > F.to_timestamp(F.lit(cutoff))).count()
    event_order = Window.partitionBy("source", "app", "event_id").orderBy("accepted_at", "seq")
    dedup = valid.withColumn("rn", F.row_number().over(event_order)).filter("rn=1").drop("rn", "_corrupt_record")
    dedup = dedup.withColumn("business_key", F.when(F.col("event_type") == "request_complete", F.concat(F.lit("request:"), F.col("request_id"))).otherwise(F.concat(F.lit("event:"), F.col("event_id"))))
    request_order = Window.partitionBy("source", "app", "business_key").orderBy("accepted_at", "seq")
    result = dedup.withColumn("rn", F.row_number().over(request_order)).filter("rn=1").drop("rn").cache()
    counts = dict(raw=raw.count(), valid=result.count(), quarantined=quarantine.count(), after_cutoff=excluded)
    counts["duplicates"] = counts["raw"] - counts["valid"] - counts["quarantined"] - counts["after_cutoff"]
    raw.unpersist()
    return result, counts, quarantine, snapshot
