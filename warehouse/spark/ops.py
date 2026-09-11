"""Distributed SCD2 and ticket episode snapshots from archived Debezium changes."""
import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--input", required=True)
parser.add_argument("--output", required=True)
parser.add_argument("--as-of", required=True, help="Business date for daily snapshots")
args = parser.parse_args()
spark = SparkSession.builder.appName("snow-operations-model").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
changes = spark.read.json(args.input).filter("source = 'synthetic'")
# Kafka source positions, not mutable business timestamps, remove CDC delivery retries.
changes = changes.dropDuplicates(["kafka_topic", "kafka_partition", "kafka_offset"]).withColumn("at", F.to_timestamp("at"))
order = Window.partitionBy("source", "table", "key").orderBy("at", "version", "kafka_offset")
same_effective_time = Window.partitionBy("source", "table", "key", "at").orderBy(F.desc("version"), F.desc("kafka_offset"))
contents = changes.filter("table = 'contents'").withColumn("latest", F.row_number().over(same_effective_time)).filter("latest=1").drop("latest")
contents = contents.withColumn("valid_to", F.lead("at").over(order))
contents = contents.select("source", "key", F.col("at").alias("valid_from"), "valid_to", "version", "after", (F.col("op") == "d").alias("deleted"))
contents.write.mode("errorifexists").parquet(args.output + "/dim_content_scd2")
tickets = changes.filter("table = 'tickets'").withColumn("previous_status", F.lag("after.status").over(order))
tickets = tickets.withColumn("starts_round", F.when((F.col("after.status") == "open") & (F.col("previous_status").isNull() | (F.col("previous_status") == "resolved")), 1).otherwise(0))
tickets = tickets.withColumn("round", F.sum("starts_round").over(order.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
rounds = tickets.groupBy("source", "key", "round").agg(
    F.min(F.when(F.col("starts_round") == 1, F.col("at"))).alias("opened_at"),
    F.min(F.when(F.col("after.status") == "resolved", F.col("at"))).alias("resolved_at"))
rounds = rounds.withColumn("duration_seconds", F.unix_timestamp("resolved_at") - F.unix_timestamp("opened_at"))
ticket_scope = Window.partitionBy("source", "key")
rounds = rounds.withColumn("first_resolved_at", F.min("resolved_at").over(ticket_scope)).withColumn("latest_resolved_at", F.max("resolved_at").over(ticket_scope))
if rounds.filter("opened_at IS NULL OR duration_seconds < 0").count():
    raise RuntimeError("Invalid ticket state transitions; refuse snapshot publication")
rounds.write.mode("errorifexists").parquet(args.output + "/fact_ticket_round")
dated = tickets.withColumn("day", F.to_date(F.from_utc_timestamp("at", "Asia/Hong_Kong")))
calendar = dated.groupBy("source", "key").agg(F.min("day").alias("start")).withColumn("day", F.explode(F.sequence("start", F.to_date(F.lit(args.as_of)))))
daily = calendar.alias("c").join(dated.alias("e"), (F.col("c.source") == F.col("e.source")) & (F.col("c.key") == F.col("e.key")) & (F.col("e.day") <= F.col("c.day")))
daily = daily.select("c.source", "c.key", "c.day", "e.at", "e.version", "e.op", "e.after")
latest = Window.partitionBy("source", "key", "day").orderBy(F.desc("at"), F.desc("version"))
daily.withColumn("rn", F.row_number().over(latest)).filter("rn=1").drop("rn").write.mode("errorifexists").parquet(args.output + "/fact_ticket_daily")
spark.stop()
