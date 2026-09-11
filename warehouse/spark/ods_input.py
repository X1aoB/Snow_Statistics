"""Python 3.8-compatible frozen ODS input resolution, shared by Spark jobs."""
import hashlib
import json
import re


def resolve_snapshot(path, body, kind):
    snapshot = json.loads(body)
    token = hashlib.sha256(json.dumps(snapshot, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()).hexdigest()
    root = snapshot["root"]
    if (snapshot["schema_version"] != 1 or snapshot["source"] != "synthetic" or
            not re.fullmatch(r"hdfs://[a-zA-Z0-9.-]+:9000/snow/ods/synthetic/kafka/[a-z0-9-]{1,60}", root) or
            path != root + "/snapshots/" + token + "/_snapshot.json" or kind not in ("events", "changes")):
        raise ValueError("Invalid ODS snapshot identity/path")
    paths, seen = [], set()
    for entry in snapshot["batches"]:
        batch = entry["batch_id"]
        if not re.fullmatch(r"[0-9a-f]{64}", batch) or batch in seen or entry["counts"]["quarantine"]:
            raise ValueError("ODS snapshot has duplicate batches or quarantined input")
        seen.add(batch)
        if entry["counts"][kind]:
            paths.append(root + "/batches/" + batch + "/" + kind + ".jsonl")
    if not paths:
        raise ValueError("Snapshot has no input records for this model")
    return paths, dict(snapshot_id=token, offsets=snapshot["offsets"], batches=len(seen), source="synthetic")


def spark_inputs(spark, path, kind):
    if not path.endswith("/_snapshot.json"):
        return path, None
    jpath = spark._jvm.org.apache.hadoop.fs.Path(path)
    fs = jpath.getFileSystem(spark._jsc.hadoopConfiguration())
    if fs.getFileStatus(jpath).getLen() > 4 * 1024 * 1024:
        raise ValueError("ODS snapshot exceeds bounded manifest size")
    stream = fs.open(jpath)
    try:
        body = spark._jvm.org.apache.commons.io.IOUtils.toString(stream, "UTF-8")
    finally:
        stream.close()
    return resolve_snapshot(path, body, kind)
