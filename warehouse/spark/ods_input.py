"""Python 3.8-compatible frozen ODS input resolution, shared by Spark jobs."""
import hashlib
import json
import re
from datetime import datetime, timezone
from uuid import UUID


def resolve_snapshot(path, body, kind, source="synthetic", now=None):
    snapshot = json.loads(body)
    token = hashlib.sha256(json.dumps(snapshot, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()).hexdigest()
    root = snapshot["root"]
    if (source not in ("real", "synthetic") or snapshot["schema_version"] != (2 if source == "real" else 1) or snapshot["source"] != source or
            not re.fullmatch(r"hdfs://[a-zA-Z0-9.-]+:9000/snow/ods/" + source + r"/kafka/[a-z0-9-]{1,60}", root) or
            path != root + "/snapshots/" + token + "/_snapshot.json" or kind not in ("events", "changes")):
        raise ValueError("Invalid ODS snapshot identity/path")
    if source == "real" and kind != "events":
        raise ValueError("Real input does not contain simulated CDC")
    collector = None
    if source == "real":
        collector = snapshot["identity"].get("collector")
        if (not isinstance(collector, dict) or set(collector) != {"schema_version", "source", "instance_id", "generation"}
                or type(collector["schema_version"]) is not int or collector["schema_version"] != 1 or collector["source"] != "real"):
            raise ValueError("Real ODS snapshot lacks explicit collector identity")
        for key in ("instance_id", "generation"):
            if not isinstance(collector[key], str) or str(UUID(collector[key])) != collector[key]:
                raise ValueError("Invalid collector generation metadata")
    paths, seen = [], set()
    for entry in snapshot["batches"]:
        batch = entry["batch_id"]
        if not re.fullmatch(r"[0-9a-f]{64}", batch) or batch in seen or entry["counts"]["quarantine"]:
            raise ValueError("ODS snapshot has duplicate batches or quarantined input")
        seen.add(batch)
        if source == "real" and datetime.fromisoformat(entry["expires_at"]) <= (now or datetime.now(timezone.utc)):
            raise ValueError("Real ODS window expired; cleanup must run before reads")
        if entry["counts"][kind]:
            paths.append(root + "/batches/" + batch + "/" + kind + ".jsonl")
    if not paths:
        raise ValueError("Snapshot has no input records for this model")
    metadata = dict(snapshot_id=token, offsets=snapshot["offsets"], batches=len(seen), source=source)
    if collector is not None:
        metadata["collector"] = collector.copy()
    return paths, metadata


def spark_inputs(spark, path, kind, source="synthetic"):
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
    return resolve_snapshot(path, body, kind, source)
