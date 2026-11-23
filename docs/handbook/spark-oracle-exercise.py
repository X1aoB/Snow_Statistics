"""Synthetic Python oracle exercise; never starts Spark or contacts a service."""
import json
from pathlib import Path
from uuid import uuid4

from snow_statistics.model import build, deduplicate
from snow_statistics.simulator import generate

folder = Path("runtime/handbook/ch17-20261013/runs") / uuid4().hex
folder.mkdir(parents=True, exist_ok=False)
fixture = generate(seed=42, users=2)
raw = fixture["events"] * 2
valid, quality, quarantine = deduplicate(raw)
actual = build({"events": raw, "changes": fixture["changes"]})
reference = build(fixture)
assert quality == {"raw": 56, "valid": 28, "duplicates": 28, "quarantined": 0}
assert not quarantine and actual["daily"] == reference["daily"]
assert len(actual["daily"]) == 6
for name, rows in (("ods.synthetic.jsonl", raw), ("dwd.synthetic.jsonl", valid)):
    (folder / name).write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
summary = {"engine": "Python correctness oracle; Spark not executed", "source": "synthetic",
           "seed": 42, "quality": quality, "daily": actual["daily"], "daily_matches_single_copy": True}
(folder / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
print(json.dumps({"output": str(folder), **summary}, indent=2))
