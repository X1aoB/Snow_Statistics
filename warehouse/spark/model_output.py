"""Small accepted manifests and optional local packages; Python 3.8 compatible."""
import json
import os
from datetime import date, datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F


def jsonable(value):
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [jsonable(v) for v in value]
    return value


def records(frame, limit=10000):
    rows = frame.limit(limit + 1).collect()
    if len(rows) > limit:
        raise ValueError("Bounded report/golden readback limit exceeded")
    return sorted([jsonable(r.asDict(recursive=True)) for r in rows], key=lambda r: json.dumps(r, sort_keys=True))


def register(spark, table, path, count):
    spark.sql("CREATE DATABASE IF NOT EXISTS snow_synthetic")
    spark.sql("CREATE TABLE " + table + " USING PARQUET LOCATION '" + path.replace("'", "''") + "'")
    if spark.table(table).count() != count:
        raise ValueError("Hive catalog readback mismatch")


def accept(spark, output, package, local_path=None):
    spark.range(1).coalesce(1).select(F.lit(json.dumps(package)).alias("value")).write.mode("errorifexists").text(output + "/accepted")
    if local_path:
        target = Path(local_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        temp = target.with_suffix(".tmp")
        with temp.open("w", encoding="utf-8") as stream:
            json.dump(package, stream)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, target)
        descriptor = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
