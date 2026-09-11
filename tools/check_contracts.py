"""Contract artifacts must match code; intentional contract changes are reviewable."""
import json
from pathlib import Path

from snow_statistics.contracts import Batch, Summary

root = Path(__file__).resolve().parents[1] / "contracts/v1"
for name, model in [("events.schema.json", Batch), ("summary.schema.json", Summary)]:
    assert json.loads((root / name).read_text()) == model.model_json_schema(), f"Stale contract: {name}"
print("v1 contract artifacts match the implementation")
