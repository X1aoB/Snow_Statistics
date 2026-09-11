"""Materialize verified archived batches as immutable Spark ODS JSON Lines."""
import argparse
import json
from pathlib import Path

from snow_statistics.io import atomic_write, digest

parser = argparse.ArgumentParser()
parser.add_argument("directory", type=Path)
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()
lines = []
for manifest_path in sorted((args.directory / "batches").glob("*.manifest.json")):
    manifest = json.loads(manifest_path.read_text())
    payload = (args.directory / manifest["archive"]).read_bytes()
    if digest(payload) != manifest["sha256"]:
        raise ValueError("Archive checksum failed")
    lines.extend(json.dumps(row) for row in json.loads(payload)["events"])
atomic_write(args.output, ("\n".join(lines) + "\n").encode())
print(f"Exported {len(lines)} source envelopes")
