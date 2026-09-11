"""Real Kafka sink with an isolated synthetic collector and injected publish crash."""
import json
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from kafka import KafkaProducer

from snow_statistics.config import Settings
from snow_statistics.contracts import Event
from snow_statistics.io import write_json
from snow_statistics.simulator import generate
from snow_statistics.store import Store
from snow_statistics.sync import sync_once

rows = generate(users=2)["events"]
events = [Event.model_validate(row["event"]) for row in rows]
end = max(event.occurred_at for event in events) + timedelta(seconds=1)
producer = KafkaProducer(bootstrap_servers="127.0.0.1:9092", acks="all", value_serializer=lambda row: json.dumps(row).encode())
with TemporaryDirectory() as temporary:
    settings = Settings(mode="full", source="synthetic", db=Path(temporary) / "lite.db", allowed_characters=frozenset({"sample_character"}))
    store = Store(settings, clock=lambda: end)
    store.ingest(events)
    published = 0
    def publish(row):
        global published
        producer.send("snow.synthetic.events.v1", key=row["event"]["event_id"].encode(), value=row).get(timeout=20)
        published += 1
    def fail(row):
        publish(row)
        if published == 5:
            raise RuntimeError("injected crash after sink acknowledgement")
    archive = Path(temporary) / "archive"
    try:
        sync_once(archive, store.read, fail)
    except RuntimeError:
        pass
    assert not (archive / "cursor.json").exists()
    recovered = sync_once(archive, store.read, publish)
    assert recovered == len(rows) and published == len(rows) + 5
    while store.aggregate():
        pass
    assert store.summary().daily == []  # Synthetic events never enter public JSON.
    producer.close()
    store.close()
receipt = {"source": "synthetic", "accepted": len(rows), "kafka_acked_including_replay": published,
           "recovered_cursor": recovered, "archive_before_publish": True, "synthetic_public_rows": 0}
write_json("runtime/sync-receipt.json", receipt)
print(json.dumps(receipt))
