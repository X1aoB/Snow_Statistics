"""Cleanup-only HDFS adapter. It may inspect expired tokens solely to prune them."""
from datetime import datetime, timedelta, timezone

from pyspark.sql import functions as F
from real_support import AUX_FIELDS, auxiliary_manifest, real_path, stamp, validate_auxiliary_manifest


def prune_auxiliary_hdfs(spark, manifest, output, now):
    """Rewrite, compare, then delete one registered old real auxiliary directory.

    The caller must register ``output`` and journal its manifest before invoking
    this function. Failed deletion raises; a model read permit must not be issued.
    This adapter never deletes a parent, synthetic path, table or other VM data.
    """
    identity = {key: manifest[key] for key in ("instance_id", "generation")}
    validate_auxiliary_manifest(manifest, identity, datetime.min.replace(tzinfo=timezone.utc))
    old = real_path(manifest["path"], "auxiliary")
    real_path(output, "auxiliary")
    if old == output or output.startswith(old + "/") or old.startswith(output + "/"):
        raise ValueError("Cleanup requires disjoint registered real directories")
    frame = spark.read.parquet(old)
    if set(frame.columns) != set(AUX_FIELDS):
        raise ValueError("Auxiliary cleanup field allowlist failed")
    if frame.filter((F.col("source") != "real") | F.col("source").isNull() | F.to_timestamp("accepted_at").isNull()).count():
        raise ValueError("Auxiliary cleanup source/timestamp validation failed")
    if frame.count() != manifest["rows"]:
        raise ValueError("Auxiliary row count differs from registered manifest")
    kept = frame.filter(F.to_timestamp("accepted_at") + F.expr("INTERVAL 30 DAYS") > F.lit(now)).cache()
    kept.coalesce(2).write.mode("errorifexists").parquet(output)
    materialized = spark.read.parquet(output).select(*AUX_FIELDS)
    if kept.select(*AUX_FIELDS).exceptAll(materialized).count() or materialized.exceptAll(kept.select(*AUX_FIELDS)).count():
        raise ValueError("Auxiliary cleanup readback differs; old path retained")
    count = materialized.count()
    oldest = materialized.agg(F.min("accepted_at")).first()[0] if count else None
    retained_from = max(stamp(manifest["retained_from"]), now - timedelta(days=30)).isoformat()
    coverage = dict(identity, through=manifest["cutoff"])
    result = auxiliary_manifest(output, coverage, oldest, count, retained_from)
    jpath = spark._jvm.org.apache.hadoop.fs.Path(old)
    fs = jpath.getFileSystem(spark._jsc.hadoopConfiguration())
    if not fs.delete(jpath, True) or fs.exists(jpath):
        raise RuntimeError("Old real auxiliary directory was not removed; reads remain blocked")
    kept.unpersist()
    return result
