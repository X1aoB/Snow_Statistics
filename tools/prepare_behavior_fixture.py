"""Synthetic boundary fixture and independently computed expected outputs."""
import json
from pathlib import Path

from snow_statistics.behavior import analyze
from snow_statistics.behavior_fixture import generate_behavior
from snow_statistics.io import atomic_write, write_json

fixture = generate_behavior()
folder = Path("runtime/behavior-fixture")
atomic_write(folder / "events.jsonl", b"".join(json.dumps(r).encode() + b"\n" for r in fixture["events"]))
write_json(folder / "expected.json", {"behavior": analyze(fixture["events"], fixture["date_from"], fixture["date_to"], fixture["cutoff"])})
print(json.dumps({"events": len(fixture["events"]), "window": {k: fixture[k] for k in ("date_from", "date_to", "cutoff")}}))
