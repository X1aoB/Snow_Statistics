"""Real event-only behavior, with bounded observation state and explicit gaps.

Synthetic operations/CDC are deliberately absent. Expired auxiliary snapshots
must be pruned by the registered lifecycle before this reader is started.
"""
import argparse
import json
import os
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from event_input import read_events
from model_output import accept, records
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from real_support import (
    AUX_FIELDS,
    auxiliary_manifest,
    observation_status,
    real_path,
    stamp,
    validate_auxiliary_manifest,
    validate_coverage,
)

parser = argparse.ArgumentParser()
for name in ("input", "output", "run-id", "date-from", "date-to", "cutoff"):
    parser.add_argument("--" + name, required=True)
parser.add_argument("--expected")
parser.add_argument("--package-file")
parser.add_argument("--register-hive", action="store_true")
parser.add_argument("--coverage-file", required=True)
parser.add_argument("--auxiliary-input", help="Local registered auxiliary manifest, never a wildcard")
parser.add_argument("--auxiliary-output", required=True, help="Fresh HDFS path in /snow/auxiliary/real/")
parser.add_argument("--auxiliary-manifest-file", required=True)
args = parser.parse_args()
if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", args.run_id) or datetime.fromisoformat(args.cutoff.replace("Z", "+00:00")).tzinfo is None:
    raise ValueError("Invalid run ID/cutoff")
real_path(args.output, "warehouse")
real_path(args.auxiliary_output, "auxiliary")
coverage = validate_coverage(json.load(open(args.coverage_file, encoding="utf-8")), args.cutoff)
now = datetime.now(timezone.utc)
previous = None
if args.auxiliary_input:
    previous = validate_auxiliary_manifest(json.load(open(args.auxiliary_input, encoding="utf-8")), coverage, now)
    if stamp(previous["cutoff"]) > stamp(args.cutoff):
        raise ValueError("Cannot replay older cutoff using newer auxiliary state")
spark = SparkSession.builder.appName("snow-real-behavior-model").enableHiveSupport().getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.shuffle.partitions", "4")
through = spark.range(1).select(F.date_sub(F.to_date(F.from_utc_timestamp(F.to_timestamp(F.lit(args.cutoff)), "Asia/Hong_Kong")), 1)).first()[0]
if not date.fromisoformat(args.date_from) <= date.fromisoformat(args.date_to) <= through:
    raise ValueError("Report dates must be closed HK dates at cutoff")
events, quality, _, snapshot = read_events(spark, args.input, "real", args.cutoff)
if not snapshot or snapshot["source"] != "real":
    raise ValueError("Real behavior requires an immutable real ODS snapshot")
coverage = validate_coverage(coverage, args.cutoff, snapshot["collector"])
if quality["quarantined"]:
    raise ValueError("ODS quality gate failed")
retained_from = max(stamp(coverage["continuous_from"]), now - timedelta(days=30),
                    stamp(previous["retained_from"]) if previous else now - timedelta(days=7))
minimal = events.select(*AUX_FIELDS)
if previous and previous["rows"]:
    old = spark.read.parquet(previous["path"])
    if set(old.columns) != set(AUX_FIELDS) or old.filter(F.col("source") != "real").count():
        raise ValueError("Auxiliary state contains unexpected fields/source")
    if old.filter(F.to_timestamp("accepted_at") + F.expr("INTERVAL 30 DAYS") <= F.lit(now)).count():
        raise ValueError("Auxiliary rows expired; cleanup must precede all readers")
    minimal = minimal.unionByName(old)
minimal = minimal.filter(F.to_timestamp("accepted_at") + F.expr("INTERVAL 30 DAYS") > F.lit(now))
minimal = minimal.withColumn("key", F.when(F.col("event_type") == "request_complete", F.concat(F.lit("request:"), F.col("request_id"))).otherwise(F.concat(F.lit("event:"), F.col("event_id"))))
dedup_order = Window.partitionBy("source", "app", "key").orderBy("accepted_at", "seq")
minimal = minimal.withColumn("rn", F.row_number().over(dedup_order)).filter("rn=1").drop("key", "rn").cache()
minimal.coalesce(2).write.mode("errorifexists").parquet(args.auxiliary_output)
auxiliary_count = minimal.count()
oldest = minimal.agg(F.min("accepted_at")).first()[0] if auxiliary_count else None
aux_manifest = auxiliary_manifest(args.auxiliary_output, coverage, oldest, auxiliary_count, retained_from.isoformat())
events = minimal.withColumn("event_time", F.to_timestamp("occurred_at")).filter(F.col("event_time") <= F.to_timestamp(F.lit(args.cutoff))).cache()
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
    # Bounded day metadata is evaluated by the independently tested coverage
    # helper on the driver; no Python UDF or user identifiers leave executors.
    condition = F.lit("pending")
    for item in retention.select("cohort_date").distinct().collect():
        label = str(item[0])
        state = observation_status(label, lag, args.cutoff, coverage, retained_from.isoformat())
        condition = F.when(F.col("cohort_date") == label, F.lit(state)).otherwise(condition)
    retention = retention.withColumn("observation_d" + str(lag), condition)
    mature = F.col("observation_d" + str(lag)) == "complete_accepted_prefix"
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
        table = "snow_real.behavior_" + args.run_id.replace("-", "_") + "_" + name
        spark.sql("CREATE DATABASE IF NOT EXISTS snow_real")
        spark.sql("CREATE TABLE " + table + " USING PARQUET LOCATION '" + target + "'")
        if spark.table(table).count() != counts[name]:
            raise ValueError("Hive real catalog readback mismatch")
        tables[name] = table
if args.expected:
    expected = json.load(open(args.expected, encoding="utf-8"))["behavior"]
    for name, frame in materialized.items():
        if records(frame) != sorted(expected[name], key=lambda r: json.dumps(r, sort_keys=True)):
            raise ValueError("Materialized " + name + " differs from independent golden model")
manifest = dict(schema_version=1, kind="real_behavior", source="real", run_id=args.run_id,
                input=args.input, input_snapshot=snapshot, output=args.output, date_from=args.date_from,
                date_to=args.date_to, cutoff=args.cutoff, complete_through=str(through),
                cohort_definition="first_observed_in_retained_30d_window", observation_scope="accepted_events_only",
                coverage=coverage, retained_from=retained_from.isoformat(), auxiliary=aux_manifest,
                quality=quality, counts=counts, hive_tables=tables,
                engine="Spark " + spark.version, master=spark.sparkContext.master,
                application_id=spark.sparkContext.applicationId, golden_equal=bool(args.expected),
                generated_at=datetime.now(timezone.utc).isoformat())
resources = [dict(source="real", kind="auxiliary", path=args.output + "/" + name,
                  original_min_accepted_at=oldest, expires_at=aux_manifest["expires_at"])
             for name in ("sessions", "conversions")]
resources.extend(dict(source="real", kind="aggregate", path=args.output + "/" + name,
                      original_min_accepted_at=None,
                      expires_at=(datetime.combine(date.fromisoformat(args.date_from), datetime.min.time(), timezone(timedelta(hours=8))) + timedelta(days=90)).isoformat())
                 for name in ("session_daily", "retention", "funnel", "accepted"))
manifest["resources"] = [aux_manifest] + resources
package = dict(schema_version=2, manifest=manifest,
               aggregates={name: records(materialized[name]) for name in ("session_daily", "retention", "funnel")})
accept(spark, args.output, package, args.package_file)
Path(args.auxiliary_manifest_file).parent.mkdir(parents=True, exist_ok=True)
# Only metadata; the associated parquet contains anonymous auxiliary tokens.
temporary = Path(args.auxiliary_manifest_file).with_suffix(".tmp")
with temporary.open("w", encoding="utf-8") as stream:
    json.dump(aux_manifest, stream)
    stream.flush()
    os.fsync(stream.fileno())
os.replace(temporary, args.auxiliary_manifest_file)
print(json.dumps(manifest))
spark.stop()
