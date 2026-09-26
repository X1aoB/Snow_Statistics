"""Contract artifacts must match code; intentional contract changes are reviewable."""
import json
from pathlib import Path

from snow_statistics.contracts import Batch, PublicSummary, Summary

root = Path(__file__).resolve().parents[1] / "contracts/v1"
for name, model in [("events.schema.json", Batch), ("summary.schema.json", Summary)]:
    assert json.loads((root / name).read_text()) == model.model_json_schema(), f"Stale contract: {name}"
assert json.loads((root.parent / "v2/summary.schema.json").read_text()) == PublicSummary.model_json_schema()
print("v1 and v2 contract artifacts match the implementation")
