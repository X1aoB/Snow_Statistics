"""Bounded, immutable synthetic comparisons on accepted HDFS DWD; Spark 3.5.7."""
import argparse
import json
import os
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--attempt", required=True)
args = parser.parse_args()
if not args.attempt.replace("-", "").isalnum():
    raise ValueError("Invalid experiment identity")
out = Path("/opt/snow/runtime/optimization") / args.attempt
out.mkdir(parents=True, exist_ok=False)
source = "/snow/warehouse/scale/runs/scale-1m-01/dwd"
root = "/snow/warehouse/optimization/" + args.attempt
spark = SparkSession.builder.appName("Snow Statistics synthetic optimization " + args.attempt).getOrCreate()
sc = spark.sparkContext
settings = {
    "spark.sql.session.timeZone": "UTC",
    "spark.sql.adaptive.enabled": "false",
    "spark.sql.shuffle.partitions": "4",
    "spark.sql.autoBroadcastJoinThreshold": "-1",
    "spark.sql.files.maxPartitionBytes": "33554432",
    "spark.sql.files.openCostInBytes": "4194304",
}
for key, value in settings.items():
    spark.conf.set(key, value)


def save(name, value):
    tmp = out / (name + ".tmp")
    with tmp.open("w", encoding="utf-8") as stream:
        json.dump(value, stream, sort_keys=True, indent=2)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(tmp, out / name)


def inventory(path):
    jpath = sc._jvm.org.apache.hadoop.fs.Path(path)
    fs = jpath.getFileSystem(sc._jsc.hadoopConfiguration())
    iterator = fs.listFiles(jpath, True)
    files = []
    while iterator.hasNext():
        status = iterator.next()
        if status.getPath().getName().endswith(".parquet"):
            files.append(dict(path=status.getPath().toString(), bytes=status.getLen(),
                              checksum=fs.getFileChecksum(status.getPath()).toString()))
    return sorted(files, key=lambda item: item["path"])


def plan_metrics(node):
    metrics = []
    values = node.metrics().iterator()
    while values.hasNext():
        pair = values.next()
        metric = pair._2()
        metrics.append(dict(key=pair._1(), value=metric.value(), name=str(metric.name())))
    children = node.children().iterator()
    result = [dict(node=node.nodeName(), metrics=metrics)]
    while children.hasNext():
        result.extend(plan_metrics(children.next()))
    return result


def collected(df):
    return sorted([r.asDict() for r in df.collect()], key=lambda r: json.dumps(r, sort_keys=True))


def stats(df):
    return df.agg(F.count("*").alias("rows"), F.sum("seq").alias("seq_sum"),
                  F.sum(F.length("event_id")).alias("id_length_sum"))


def timed(name, factory, expected):
    spark.catalog.clearCache()
    sc.setJobGroup(name, name)
    df = factory()
    started = time.monotonic()
    rows = collected(df)
    elapsed = time.monotonic() - started
    if rows != expected:
        save(name + "-mismatch.json", dict(expected=expected, actual=rows))
        raise RuntimeError("Result changed in " + name)
    plan = df._jdf.queryExecution().executedPlan()
    result = dict(name=name, seconds=round(elapsed, 6), rows=rows,
                  physical_plan=spark._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), "formatted"),
                  operator_metrics=plan_metrics(plan))
    save(name + ".json", result)
    print(json.dumps(dict(name=name, seconds=result["seconds"])), flush=True)
    return result


try:
    sc.setJobGroup("setup-verify", "Verify immutable accepted million-row fixture")
    accepted = json.loads(Path("/opt/snow/runtime/scale/scale-1m-01.json").read_bytes())
    assert accepted["manifest"]["quality"]["valid"] == 900000
    assert accepted["manifest"]["master"] == "yarn" and sc.master == "yarn"
    before = inventory(source)
    assert before
    facts = spark.read.parquet(source)
    assert facts.count() == 900000 and facts.filter("source <> 'synthetic'").count() == 0
    all_stats = collected(stats(facts))
    day = "2026-01-02"
    expected_day = collected(stats(facts.filter(F.col("business_date") == day)))
    roles = facts.filter(F.col("character_id").isNotNull()).select("seq", "event_id", "character_id")
    dimension = spark.createDataFrame([("hot_character", "popular", 10), ("other_character", "other", 3)],
                                      "character_id string, category string, weight long")
    expected_roles = [dict(category="other", rows=15000, weight_sum=45000),
                      dict(category="popular", rows=285000, weight_sum=2850000)]

    def join(strategy):
        if strategy == "salted":
            left = roles.withColumn("salt", F.pmod(F.xxhash64("event_id"), F.lit(16)))
            right = dimension.crossJoin(spark.range(16).select(F.col("id").alias("salt")))
            joined = left.hint("merge").join(right.hint("merge"), ["character_id", "salt"])
        else:
            joined = roles.hint("merge").join(
                F.broadcast(dimension) if strategy == "broadcast" else dimension.hint("merge"),
                "character_id")
        return joined.groupBy("category").agg(F.count("*").alias("rows"), F.sum("weight").alias("weight_sum"))

    # Layout preparation is outside the timed query comparisons and uses new paths only.
    sc.setJobGroup("setup-layout", "Create deliberately fragmented and compact copies")
    projection = facts.select("seq", "event_id", "app", "business_date", "character_id", "event_type")
    small, compact = root + "/small", root + "/compact"
    projection.repartition(64).write.mode("errorifexists").parquet(small)
    spark.read.parquet(small).coalesce(4).write.mode("errorifexists").parquet(compact)
    small_files, compact_files = inventory(small), inventory(compact)
    assert len(small_files) == 64 and len(compact_files) == 4
    # Exact multiset comparison, not just matching aggregate counters.
    sc.setJobGroup("setup-equivalence", "Verify both layout copies preserve every projected row")
    for path in (small, compact):
        materialized = spark.read.parquet(path).select(projection.columns)
        assert not projection.exceptAll(materialized).take(1)
        assert not materialized.exceptAll(projection).take(1)

    # Observe the hot-key distribution independently of wall-clock speedup.
    sc.setJobGroup("setup-skew", "Observe rows across four exchange partitions")
    distribution = {}
    for kind, repartitioned in (
        ("key_only", roles.repartition(4, "character_id")),
        ("salted", roles.withColumn("salt", F.pmod(F.xxhash64("event_id"), F.lit(16)))
         .repartition(4, "character_id", "salt")),
    ):
        distribution[kind] = collected(repartitioned.withColumn("bucket", F.spark_partition_id())
                                        .groupBy("bucket").agg(F.count("*").alias("rows")))
        assert sum(r["rows"] for r in distribution[kind]) == 300000
    save("setup.json", dict(source=source, accepted_application=accepted["manifest"]["application_id"],
                             source_files=before, small_files=small_files, compact_files=compact_files,
                             exact_multiset_equal=True, all_stats=all_stats, hot_key_distribution=distribution))

    cases = {
        "partition_expression": (lambda: stats(facts.filter(F.to_date(F.from_utc_timestamp("event_time", "Asia/Hong_Kong")) == day)), expected_day),
        "partition_column": (lambda: stats(facts.filter(F.col("business_date") == day)), expected_day),
        "join_merge": (lambda: join("merge"), expected_roles),
        "join_broadcast": (lambda: join("broadcast"), expected_roles),
        "join_salted": (lambda: join("salted"), expected_roles),
        "files_small": (lambda: stats(spark.read.parquet(small)), all_stats),
        "files_compact": (lambda: stats(spark.read.parquet(compact)), all_stats),
    }
    results = []
    # Three measured rounds with reversed middle order. OS/HDFS caches are not cleared.
    for number in range(3):
        names = list(cases) if number != 1 else list(reversed(cases))
        for name in names:
            factory, expected = cases[name]
            results.append(timed(name + "-" + str(number + 1), factory, expected))
    assert inventory(source) == before
    receipt = dict(schema_version=1, attempt=args.attempt, source="synthetic", engine=spark.version,
                   master=sc.master, application_id=sc.applicationId, input_rows=900000, settings=settings,
                   result_rows_equal=True, source_unchanged=True, cases=[r["name"] for r in results],
                   note="Three ordered/reversed rounds; no Spark cache, OS/HDFS cache retained; one executor/core. Query timing excludes setup and result-equivalence checks; no production throughput claim.")
    save("accepted.json", receipt)
finally:
    spark.stop()
