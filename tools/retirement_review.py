"""Print an inspectable retirement scope. Deliberately contains no delete operation."""
import argparse
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("mode", choices=["lite", "off", "delete"])
args = parser.parse_args()
root = Path(__file__).resolve().parents[1]
manifest = json.loads((root / "deploy/resources.json").read_text())
print(manifest["retirement"][args.mode])
print(json.dumps(manifest["local"] if args.mode == "lite" else manifest, indent=2))
print("Review only: no files, containers, databases or business resources were changed.")
