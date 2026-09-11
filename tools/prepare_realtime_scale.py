"""Reuse the exact accepted 100k YARN fixture in safe event-time replay order."""

import argparse
import gzip
import hashlib
import json
from pathlib import Path

from snow_statistics.history_replay import order_history
from snow_statistics.io import write_json

parser = argparse.ArgumentParser()
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()
if args.output.exists():
    parser.error("Use a new replay directory; existing inputs and receipts are retained")
source = Path("runtime/scale/input-100k-v1")
manifest = json.loads((source / "manifest.json").read_bytes())
accepted = json.loads(Path("runtime/scale/scale-100k-01.json").read_bytes())
assert (
    accepted["manifest"]["master"] == "yarn"
    and manifest["source"] == "synthetic"
    and manifest["events"] == 100000
)
assert accepted["manifest"]["quality"] == dict(
    raw=100000, valid=90000, duplicates=10000, quarantined=0, after_cutoff=0
)
rows = []
for i, shard in enumerate(manifest["shards"]):
    assert shard["name"] == f"events-{i:02d}.jsonl.gz"
    path = source / shard["name"]
    assert (
        path.stat().st_size == shard["bytes"]
        and hashlib.sha256(path.read_bytes()).hexdigest() == shard["sha256"]
    )
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        rows.extend(json.loads(line) for line in stream)
assert len(rows) == 100000 and {row["seq"] for row in rows} == set(range(1, 100001))
ordered = order_history(rows)
args.output.mkdir(parents=True)
path = args.output / "events.jsonl.gz"
with path.open("wb") as raw, gzip.GzipFile(filename="", fileobj=raw, mode="wb", mtime=0) as output:
    for row in ordered:
        output.write((json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n").encode())
write_json(
    args.output / "expected.json", dict(daily=accepted["daily"], quality=accepted["manifest"]["quality"])
)
write_json(
    args.output / "manifest.json",
    dict(
        schema_version=1,
        source="synthetic",
        events=100000,
        ordering="occurred_at then original seq; first event/request winners validated unchanged",
        original_positions_preserved=True,
        replay_bytes=path.stat().st_size,
        replay_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        original_shards=manifest["shards"],
        yarn_application_id=accepted["manifest"]["application_id"],
        note="Historical replay in a new lane, not original arrival disorder or production freshness",
    ),
)
print(
    json.dumps(
        dict(
            events=100000,
            output=str(args.output),
            replay_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        )
    )
)
