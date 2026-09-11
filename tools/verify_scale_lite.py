"""Historical fixed-clock comparison through the actual lightweight store/aggregator."""

import argparse
import gzip
import json
import time
from pathlib import Path

from snow_statistics.config import Settings
from snow_statistics.contracts import Event
from snow_statistics.io import write_json
from snow_statistics.store import Store

parser = argparse.ArgumentParser()
parser.add_argument("--directory", type=Path, required=True)
args = parser.parse_args()
path = args.directory / "lite/statistics.db"
if path.exists():
    parser.error("Existing historical store is preserved; choose a new experiment")
clock = [None]
store = Store(
    Settings(
        db=path,
        mode="lite",
        source="synthetic",
        allowed_characters=frozenset({"hot_character", "other_character"}),
    ),
    clock=lambda: clock[0],
)
accepted = duplicates = total = 0
started = time.monotonic()
try:
    batch = []
    with gzip.open(args.directory / "events.jsonl.gz", "rt", encoding="utf-8") as stream:
        for line in stream:
            row = json.loads(line)
            batch.append(Event.model_validate(row["event"]))
            if len(batch) == 50:
                clock[0] = max(event.occurred_at for event in batch)
                result = store.ingest(batch)
                accepted += result["accepted"]
                duplicates += result["duplicates"]
                total += len(batch)
                batch = []
                if total % 1000 == 0:
                    while store.aggregate():
                        pass
    assert not batch and total == 100000
    while store.aggregate():
        pass
    daily = [
        dict(
            source=row["source"],
            app=row["app"],
            date=row["day"],
            **{key: row[key] for key in ("pv", "uv", "requests", "successes")},
        )
        for row in store.db.execute("SELECT * FROM daily ORDER BY source,day,app")
    ]
    assert daily == json.loads((args.directory / "expected.json").read_bytes())["daily"]
    assert not store.summary().daily
    result = dict(
        source="synthetic",
        input_events=total,
        event_id_accepted=accepted,
        event_id_duplicates=duplicates,
        daily=daily,
        public_synthetic_rows=0,
        elapsed_seconds=round(time.monotonic() - started, 3),
        note="Actual Store/aggregate with fixed clock advanced per batch; not HTTP ingestion, original receive times, production freshness or retention-capacity evidence",
    )
finally:
    store.close()
result["state_file_bytes"] = sum(p.stat().st_size for p in path.parent.iterdir() if p.is_file())
write_json(args.directory / "lite-receipt.json", result)
print(json.dumps({key: value for key, value in result.items() if key != "daily"}))
