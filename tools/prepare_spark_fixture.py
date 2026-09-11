import argparse
import json
from pathlib import Path

from snow_statistics.io import atomic_write, write_json
from snow_statistics.model import build
from snow_statistics.simulator import generate

parser = argparse.ArgumentParser()
parser.add_argument("--as-of", help="Explicit HK operations snapshot date; event metrics are unchanged")
args = parser.parse_args()
fixture = generate(users=2)
folder = Path("runtime/spark-fixture")
atomic_write(folder / "events.jsonl", ("\n".join(json.dumps(row) for row in fixture["events"] * 2) + "\n").encode())
changes = [c | {"kafka_topic": "snow.synthetic.cdc.snow_ops." + c["table"], "kafka_partition": 0, "kafka_offset": i}
           for i, c in enumerate(fixture["changes"])]
atomic_write(folder / "ops.jsonl", ("\n".join(json.dumps(c) for c in changes) + "\n").encode())
write_json(folder / "expected.json", build(fixture, operations_as_of=args.as_of))
print("Generated synthetic Spark fixtures and hand-checkable reference")
