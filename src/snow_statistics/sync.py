"""Archive before publish. A crash may replay Kafka records; it cannot skip them."""
import json
from pathlib import Path

import httpx

from .contracts import Event
from .io import digest, exclusive, write_json


def sync_once(directory: Path, fetch, publish):
    with exclusive(directory):
        state = directory / "cursor.json"
        after = json.loads(state.read_text())["cursor"] if state.exists() else 0
        pending = directory / "pending.json"
        if pending.exists():
            batch = json.loads(pending.read_text())
            body = (directory / batch["archive"]).read_bytes()
            if digest(body) != batch["sha256"]:
                raise ValueError("archive checksum or cursor mismatch")
            response = json.loads(body)
            if after == response["next_cursor"]:
                pending.unlink()
                return 0
            if batch["after"] != after:
                raise ValueError("archive cursor mismatch")
        else:
            response = fetch(after)
            rows = response["events"]
            if not rows:
                if response["next_cursor"] != after:
                    raise ValueError("empty batch cannot advance cursor")
                return 0
            seqs = [r["seq"] for r in rows]
            if seqs != list(range(after + 1, response["next_cursor"] + 1)):
                raise ValueError("non-contiguous source cursor")
            for row in rows:
                Event.model_validate(row["event"])
                if row["source"] not in {"real", "synthetic"}:
                    raise ValueError("invalid provenance")
            archive = f"batches/{after + 1:020d}-{response['next_cursor']:020d}.json"
            write_json(directory / archive, response)
            batch = {"archive": archive, "sha256": digest((directory / archive).read_bytes()), "after": after}
            write_json(directory / (archive + ".manifest.json"), batch)
            write_json(pending, batch)
        for row in response["events"]:
            publish(row)  # Must return only after a durable sink acknowledgement.
        write_json(state, {"cursor": response["next_cursor"]})
        pending.unlink()
        return len(response["events"])


def kafka_sync(url, token, bootstrap, directory):
    from kafka import KafkaProducer
    if not token:
        raise ValueError("SNOW_READER_TOKEN required")
    producer = KafkaProducer(bootstrap_servers=bootstrap, acks="all", retries=3,
                             max_in_flight_requests_per_connection=1,
                             value_serializer=lambda value: json.dumps(value).encode())
    try:
        with httpx.Client(base_url=url, headers={"Authorization": "Bearer " + token}, timeout=15) as client:
            def fetch(after):
                response = client.get("/analytics/private/v1/events", params={"after": after})
                response.raise_for_status()
                return response.json()
            def publish(row):
                event = row["event"]
                producer.send(f"snow.{row['source']}.events.v1",
                              key=f"{event['app']}:{event['event_id']}".encode(), value=row).get(timeout=20)
            return sync_once(directory, fetch, publish)
    finally:
        producer.close(timeout=10)
