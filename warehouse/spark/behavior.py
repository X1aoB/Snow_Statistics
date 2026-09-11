"""Distributed sessions, mature D1/D7 cohorts and last-entry attribution."""
import argparse
import json
import re
from datetime import date, datetime, timezone

from event_input import read_events
from model_output import accept, records, register
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()
for name in ("input", "output", "run-id", "date-from", "date-to", "cutoff"):
    parser.add_argument("--" + name, required=True)
parser.add_argument("--expected")
parser.add_argument("--package-file")
parser.add_argument("--register-hive", action="store_true")
args = parser.parse_args()
if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", args.run_id) or datetime.fromisoformat(args.cutoff.replace("Z", "+00:00")).tzinfo is None:
    raise ValueError("Invalid run ID/cutoff")
spark = SparkSession.builder.appName("snow-behavior-model").enableHiveSupport().getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.shuffle.partitions", "4")
through = spark.range(1).select(F.date_sub(F.to_date(F.from_utc_timestamp(F.to_timestamp(F.lit(args.cutoff)), "Asia/Hong_Kong")), 1)).first()[0]
if not date.fromisoformat(args.date_from) <= date.fromisoformat(args.date_to) <= through:
    raise ValueError("Report dates must be closed HK dates at cutoff")
events, quality, _, snapshot = read_events(spark, args.input, "synthetic", args.cutoff)
if quality["quarantined"]:
    raise ValueError("ODS quality gate failed")
events = events.filter(F.col("event_time") <= F.to_timestamp(F.lit(args.cutoff))).cache()
activity = events.filter("anonymous_id IS NOT NULL")
order = Window.partitionBy("source", "app", "anonymous_id").orderBy("event_time", "event_id")
activity = activity.withColumn("previous", F.lag("event_time").over(order))
activity = activity.withColumn("starts", F.when(F.col("previous").isNull() | (F.col("event_time") >= F.col("previous") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
activity = activity.withColumn("session_number", F.sum("starts").over(order.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
sessions = activity.groupBy("source", "app", "anonymous_id", "session_number").agg(
    F.min("event_time").alias("start"), F.max("event_time").alias("end"), F.count("*").alias("events")).drop("session_number")
sessions = sessions.withColumn("date", F.to_date(F.from_utc_timestamp("start", "Asia/Hong_Kong"))).filter(F.col("date").between(args.date_from, args.date_to))
sessions = sessions.withColumn("duration_seconds", (F.unix_micros("end") - F.unix_micros("start")) / 1000000).withColumn("closed", F.col("end") + F.expr("INTERVAL 30 MINUTES") <= F.to_timestamp(F.lit(args.cutoff)))
session_daily = sessions.groupBy("source", "app", "date").agg(F.count("*").alias("sessions"), F.countDistinct("anonymous_id").alias("users"),
    F.sum("events").alias("events"), F.sum("duration_seconds").alias("duration_seconds"), F.sum(F.col("closed").cast("long")).alias("closed_sessions"))

days = events.filter("anonymous_id IS NOT NULL").select("source", "app", "anonymous_id", F.to_date(F.from_utc_timestamp("event_time", "Asia/Hong_Kong")).alias("day")).filter(F.col("day") <= F.lit(through)).distinct()
people = days.groupBy("source", "app", "anonymous_id").agg(F.min("day").alias("cohort_date"))
people = people.join(days, ["source", "app", "anonymous_id"]).groupBy("source", "app", "anonymous_id", "cohort_date").agg(*[
    F.max(F.when(F.col("day") == F.date_add("cohort_date", lag), 1).otherwise(0)).alias("returned_d" + str(lag)) for lag in (1, 7)])
retention = people.groupBy("source", "app", "cohort_date").agg(F.count("*").alias("users"), *[
    F.sum("returned_d" + str(lag)).alias("retained_d" + str(lag)) for lag in (1, 7)]).filter(F.col("cohort_date").between(args.date_from, args.date_to))
for lag in (1, 7):
    mature = F.date_add("cohort_date", lag) <= F.lit(through)
    retention = retention.withColumn("eligible_d" + str(lag), F.when(mature, F.col("users")).otherwise(0)).withColumn("retained_d" + str(lag), F.when(mature, F.col("retained_d" + str(lag))).otherwise(F.lit(None).cast("long")))

clicks = events.filter("event_type='entry_click' AND app='mywebsite'").withColumn("rn", F.row_number().over(Window.partitionBy("source", "jump_id").orderBy("event_time", "event_id"))).filter("rn=1").select("source", "jump_id", "channel", F.col("event_time").alias("click_time"))
arrivals = events.filter("event_type='entry_arrival' AND app='project_snow' AND anonymous_id IS NOT NULL").select("source", "jump_id", "anonymous_id", "event_id", F.col("event_time").alias("arrival_time"))
arrivals = arrivals.join(clicks, ["source", "jump_id"]).filter("arrival_time >= click_time AND arrival_time <= click_time + INTERVAL 30 MINUTES")
arrivals = arrivals.withColumn("rn", F.row_number().over(Window.partitionBy("source", "jump_id").orderBy("arrival_time", "event_id"))).filter("rn=1").drop("rn", "event_id")
observed = events.filter("event_type='request_observed' AND app='project_snow' AND anonymous_id IS NOT NULL").withColumn("rn", F.row_number().over(Window.partitionBy("source", "request_id").orderBy("event_time", "event_id"))).filter("rn=1").select("source", "anonymous_id", "request_id", F.col("event_time").alias("observed_time"))
requested = arrivals.join(observed, ["source", "anonymous_id"]).filter("observed_time >= arrival_time AND observed_time <= click_time + INTERVAL 30 MINUTES")
selected_events = events.filter("event_type='character_select' AND app='project_snow' AND anonymous_id IS NOT NULL").select("source", "anonymous_id", F.col("event_time").alias("selected_time"))
selected = arrivals.join(selected_events, ["source", "anonymous_id"]).filter("selected_time >= arrival_time AND selected_time <= click_time + INTERVAL 30 MINUTES").select("source", "jump_id").distinct()
completions = events.filter("event_type='request_complete' AND app='project_snow' AND success='true'").select("source", "request_id", F.col("event_time").alias("complete_time"))
candidates = requested.join(completions, ["source", "request_id"]).filter("complete_time >= observed_time AND complete_time <= click_time + INTERVAL 30 MINUTES")
# Pick last eligible entry for each success BEFORE consuming at most one success per click.
candidates = candidates.withColumn("rn", F.row_number().over(Window.partitionBy("source", "request_id").orderBy(F.desc("click_time"), F.desc("jump_id")))).filter("rn=1").drop("rn")
conversions = candidates.withColumn("rn", F.row_number().over(Window.partitionBy("source", "jump_id").orderBy("complete_time", "request_id"))).filter("rn=1").select("source", "jump_id", "request_id", "channel", F.to_date(F.from_utc_timestamp("click_time", "Asia/Hong_Kong")).alias("date"))
flags = clicks
for name, frame in (("arrived", arrivals), ("selected", selected), ("requested", requested), ("converted", conversions)):
    flags = flags.join(frame.select("source", "jump_id").distinct().withColumn(name, F.lit(1)), ["source", "jump_id"], "left")
flags = flags.fillna(0).withColumn("date", F.to_date(F.from_utc_timestamp("click_time", "Asia/Hong_Kong"))).filter(F.col("date").between(args.date_from, args.date_to))
funnel = flags.groupBy("source", "channel", "date").agg(F.count("*").alias("clicks"), *[F.sum(name).alias(name) for name in ("arrived", "selected", "requested", "converted")])
conversions = conversions.filter(F.col("date").between(args.date_from, args.date_to))
if funnel.filter("converted > requested OR requested > arrived OR arrived > clicks OR selected > arrived").count():
    raise ValueError("Attribution reconciliation failed")
frames = dict(sessions=sessions, session_daily=session_daily, retention=retention, conversions=conversions, funnel=funnel)
counts, tables, materialized = {}, {}, {}
for name, frame in frames.items():
    target = args.output + "/" + name
    frame.coalesce(2).write.mode("errorifexists").parquet(target)
    materialized[name] = spark.read.parquet(target)
    counts[name] = materialized[name].count()
    if args.register_hive:
        table = "snow_synthetic.behavior_" + args.run_id.replace("-", "_") + "_" + name
        register(spark, table, target, counts[name])
        tables[name] = table
if args.expected:
    expected = json.load(open(args.expected, encoding="utf-8"))["behavior"]
    for name, frame in materialized.items():
        if records(frame) != sorted(expected[name], key=lambda r: json.dumps(r, sort_keys=True)):
            raise ValueError("Materialized " + name + " differs from independent golden model")
manifest = dict(schema_version=1, kind="behavior", source="synthetic", run_id=args.run_id,
                input=args.input, input_snapshot=snapshot, output=args.output, date_from=args.date_from,
                date_to=args.date_to, cutoff=args.cutoff, complete_through=str(through),
                cohort_definition="first_observed_in_input", quality=quality, counts=counts, hive_tables=tables,
                engine="Spark " + spark.version, master=spark.sparkContext.master,
                application_id=spark.sparkContext.applicationId, golden_equal=bool(args.expected),
                generated_at=datetime.now(timezone.utc).isoformat())
package = dict(schema_version=1, manifest=manifest,
               aggregates={name: records(materialized[name]) for name in ("session_daily", "retention", "funnel")})
accept(spark, args.output, package, args.package_file)
print(json.dumps(manifest))
spark.stop()
