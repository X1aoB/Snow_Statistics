"""Persist a bounded CDC batch before committing offsets. Replays are expected."""
import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from kafka import KafkaConsumer
from kafka.structs import OffsetAndMetadata

from snow_statistics.io import atomic_write, digest, exclusive, write_json

parser = argparse.ArgumentParser()
parser.add_argument("--bootstrap", required=True)
parser.add_argument("--output", type=Path, default=Path("runtime/cdc"))
args = parser.parse_args()
consumer = KafkaConsumer(bootstrap_servers=args.bootstrap, group_id="snow-cdc-archive-v1",
                         enable_auto_commit=False, auto_offset_reset="earliest")
consumer.subscribe(["snow.synthetic.cdc.snow_ops." + name for name in ("contents", "campaigns", "tickets")])
try:
    with exclusive(args.output):
        polled = consumer.poll(timeout_ms=10_000, max_records=500)
        offsets = {}
        for tp, records in polled.items():
            changes = []
            for record in records:
                if record.value is None:
                    continue  # Kafka tombstone follows the Debezium delete envelope.
                change = json.loads(record.value)
                after = change.get("after")
                before = change.get("before")
                row = after or before
                if not row:
                    raise ValueError("CDC row image missing")
                # MySQL DATETIME(3) uses milliseconds in Debezium's default adaptive mode.
                stamp = row["updated_at"]
                at = datetime.fromtimestamp(stamp / 1000, UTC).isoformat() if isinstance(stamp, int) else stamp
                changes.append(dict(source="synthetic", table=change["source"]["table"], key=row["id"],
                                    at=at, version=row["version"], op="d" if change["op"] == "d" else "u", after=after,
                                    kafka_topic=record.topic, kafka_partition=record.partition, kafka_offset=record.offset,
                                    transaction=change.get("transaction"), source_position=change["source"]))
            path = args.output / f"{tp.topic}-{tp.partition}-{records[0].offset}-{records[-1].offset}.jsonl"
            body = ("\n".join(json.dumps(c) for c in changes) + "\n").encode()
            atomic_write(path, body)
            write_json(path.with_suffix(".manifest.json"), {"sha256": digest(body), "topic": tp.topic,
                       "partition": tp.partition, "first": records[0].offset, "last": records[-1].offset})
            offsets[tp] = OffsetAndMetadata(records[-1].offset + 1, "", -1)
        if offsets:
            consumer.commit(offsets)
        print(json.dumps({"archived_partitions": len(offsets)}))
finally:
    consumer.close()
