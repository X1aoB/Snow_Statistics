"""Intentional schema artifact update; review the diff before committing."""
from pathlib import Path

from snow_statistics.contracts import Batch, PublicSummary, Summary
from snow_statistics.io import write_json

root = Path(__file__).resolve().parents[1] / "contracts/v1"
write_json(root / "events.schema.json", Batch.model_json_schema())
write_json(root / "summary.schema.json", Summary.model_json_schema())
write_json(root.parent / "v2/summary.schema.json", PublicSummary.model_json_schema())
