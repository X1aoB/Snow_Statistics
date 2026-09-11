import json
from pathlib import Path

from snow_statistics.io import digest, write_json

folder = Path("runtime/cdc")
records = []
for manifest_path in folder.glob("*.manifest.json"):
    manifest = json.loads(manifest_path.read_text())
    payload = manifest_path.with_name(manifest_path.name.replace(".manifest.json", ".jsonl")).read_bytes()
    assert digest(payload) == manifest["sha256"]
    records.extend(json.loads(line) for line in payload.splitlines() if line.strip())
unique = {(r["kafka_topic"], r["kafka_partition"], r["kafka_offset"]): r for r in records}
receipt = {"raw": len(records), "unique": len(unique), "tables": sorted({r["table"] for r in unique.values()}),
           "deletes": sum(r["op"] == "d" for r in unique.values()),
           "transaction_metadata_records": sum(r["transaction"] is not None for r in unique.values())}
assert receipt["tables"] == ["campaigns", "contents", "tickets"] and receipt["deletes"] > 0
write_json("runtime/cdc-receipt.json", receipt)
print(json.dumps(receipt))
