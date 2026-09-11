"""Prepare auditable quarantine selection and a golden oracle from actual Kafka output."""
import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path

from snow_statistics.io import digest, write_json
from snow_statistics.model import daily_metrics, deduplicate

parser = argparse.ArgumentParser()
parser.add_argument("--directory", type=Path, required=True)
args = parser.parse_args()
folder = args.directory
archive = json.loads((folder / "events.json").read_bytes())
assert [r["offset"] for r in archive["rows"]] == list(range(archive["begin"], archive["end"]))
rows = [r["value"] for r in archive["rows"]]
synthetic = [r for r in rows if r["source"] == "synthetic"]
valid, quality, quarantine = deduplicate(synthetic)
bad_sequences = {r["seq"] for r in quarantine}
eligible = [r for r in synthetic if r["seq"] not in bad_sequences]
body = "".join(json.dumps(row, sort_keys=True) + "\n" for row in eligible).encode()
(folder / "correction-events.jsonl").write_bytes(body)
write_json(folder / "correction-expected.json", dict(daily=daily_metrics(valid)))
cutoff = (max(datetime.fromisoformat(r["accepted_at"]) for r in rows) + timedelta(seconds=1)).isoformat()
write_json(folder / "correction-input.json", dict(source_archive_sha256=digest((folder / "events.json").read_bytes()),
           input_sha256=digest(body), raw=len(rows), wrong_source=len(rows) - len(synthetic), quarantine=quarantine,
           spark_input=len(eligible), expected_quality=quality, cutoff=cutoff,
           late_sequences=sorted({r["value"]["seq"] for r in json.loads((folder / "late.json").read_bytes())["rows"]})))
print(json.dumps(dict(raw=len(rows), spark_input=len(eligible), corrected_facts=len(valid), cutoff=cutoff)))
