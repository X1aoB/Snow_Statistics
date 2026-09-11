import argparse
import json
from pathlib import Path

from snow_statistics.io import digest, write_json
from snow_statistics.spark_evidence import summarize

parser = argparse.ArgumentParser()
parser.add_argument("--input", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()
with args.input.open(encoding="utf-8") as stream:
    result, plans = summarize(json.loads(line) for line in stream)
plan_file = args.output.with_suffix(".plans.json")
write_json(plan_file, plans)
result.update(eventlog_sha256=digest(args.input.read_bytes()), plans_sha256=digest(plan_file.read_bytes()))
write_json(args.output, result)
print(json.dumps(result))
