"""Produce four deterministic compressed source shards; never overwrite an experiment."""
import argparse
import gzip
import hashlib
import json
from contextlib import ExitStack
from pathlib import Path

from snow_statistics.io import write_json
from snow_statistics.scale_fixture import events, expected

parser = argparse.ArgumentParser()
parser.add_argument("--events", type=int, choices=(100_000, 1_000_000), required=True)
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()
if args.events == 1_000_000:
    prior = json.loads(Path("runtime/scale/scale-100k-01.json").read_bytes())["manifest"]
    if prior["master"] != "yarn" or prior["source"] != "synthetic" or prior["quality"] != dict(raw=100_000, valid=90_000, duplicates=10_000, quarantined=0, after_cutoff=0):
        parser.error("Complete the actual 100k YARN gate first")
if args.output.exists():
    parser.error("Choose a new input directory; existing fixtures are immutable")
args.output.mkdir(parents=True)
with ExitStack() as stack:
    files = [args.output / f"events-{i:02d}.jsonl.gz" for i in range(4)]
    streams = [stack.enter_context(gzip.GzipFile(filename="", mode="wb", fileobj=stack.enter_context(path.open("wb")), mtime=0)) for path in files]
    for row in events(args.events // 10):
        streams[(row["seq"] - 1) % 4].write((json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n").encode())
write_json(args.output / "expected.json", expected(args.events // 10))
manifest = dict(schema_version=1, source="synthetic", seed=42, events=args.events, generator="scale_fixture v1",
                date_from="2026-01-01", date_to="2026-01-08", cutoff="2026-01-09T00:00:00Z",
                shards=[dict(name=p.name, bytes=p.stat().st_size, sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in files])
write_json(args.output / "manifest.json", manifest)
print(json.dumps(manifest))
