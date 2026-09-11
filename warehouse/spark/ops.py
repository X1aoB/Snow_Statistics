"""Distributed SCD2 and ticket episode snapshots from archived Debezium changes."""
import argparse
import json
from datetime import datetime, timezone

from ods_input import spark_inputs
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--input", required=True)
parser.add_argument("--output", required=True)
parser.add_argument("--as-of", required=True, help="Business date for daily snapshots")
parser.add_argument("--expected", help="Optional local golden operations model")
args = parser.parse_args()
spark = SparkSession.builder.appName("snow-operations-model").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
input_paths, input_snapshot = spark_inputs(spark, args.input, "changes")
changes = spark.read.json(input_paths).filter("source = 'synthetic'")
# Kafka source positions, not mutable business timestamps, remove CDC delivery retries.
changes = changes.dropDuplicates(["kafka_topic", "kafka_partition", "kafka_offset"]).withColumn("at", F.to_timestamp("at"))
if changes.filter("at IS NULL OR version IS NULL OR key IS NULL").count():
    raise ValueError("Invalid CDC model input")
changes = changes.filter(F.to_date(F.from_utc_timestamp("at", "Asia/Hong_Kong")) <= F.lit(args.as_of)).cache()
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
materialized = {name: spark.read.parquet(args.output + "/" + name) for name in
                ("dim_content_scd2", "fact_ticket_round", "fact_ticket_daily")}
counts = {name: frame.count() for name, frame in materialized.items()}
if args.expected:
    expected = json.load(open(args.expected, encoding="utf-8"))
    def timestamp(value):
        return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None) if value else None
    attrs = ("title", "category", "status")
    actual_dim = sorted((r.key, r.valid_from, r.valid_to, r.deleted, r.version,
                         tuple(r.after[a] for a in attrs) if r.after else None)
                        for r in materialized["dim_content_scd2"].collect())
    expected_dim = sorted((r["key"], timestamp(r["valid_from"]), timestamp(r["valid_to"]), r["deleted"], r["version"],
                           tuple(r["attributes"][a] for a in attrs) if r["attributes"] else None)
                          for r in expected["content_scd2"])
    assert actual_dim == expected_dim, "SCD2 effective intervals/attributes differ from golden model"
    fields = ("opened_at", "resolved_at", "first_resolved_at", "latest_resolved_at")
    actual_rounds = sorted((r.key, r.round, *(r[f] for f in fields), r.duration_seconds)
                           for r in materialized["fact_ticket_round"].collect())
    expected_rounds = sorted((r["ticket_id"], r["round"], *(timestamp(r[f]) for f in fields), r["duration_seconds"])
                             for r in expected["ticket_rounds"])
    assert actual_rounds == expected_rounds, "Ticket reopening rounds/timestamps differ from golden model"
    actual_daily = sorted((r.key, str(r.day), "deleted" if r.op == "d" else r.after.status) for r in materialized["fact_ticket_daily"].collect())
    expected_daily = sorted((r["ticket_id"], r["date"], r["status"]) for r in expected["ticket_daily"] if r["date"] <= args.as_of)
    assert actual_daily == expected_daily, "Ticket daily snapshots differ from golden model"
manifest = dict(schema_version=1, source="synthetic", input=args.input, input_snapshot=input_snapshot,
                output=args.output, as_of=args.as_of, counts=counts, changes=changes.count(),
                engine="Spark " + spark.version, master=spark.sparkContext.master,
                application_id=spark.sparkContext.applicationId, golden_equal=bool(args.expected),
                generated_at=datetime.now(timezone.utc).isoformat())
spark.range(1).coalesce(1).select(F.lit(json.dumps(manifest)).alias("value")).write.mode("errorifexists").text(args.output + "/accepted")
print(json.dumps(manifest))
spark.stop()
