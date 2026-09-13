"""Archive before publish. A crash may replay Kafka records; it cannot skip them."""
import json
import math
import re
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import urlsplit

import httpx

from .contracts import Event
from .io import digest, write_json
from .lifecycle import RealLifecycle
from .publication import publication_lock
from .source_cursor import SourceGap, check_source, record_gap


def sync_once(directory: Path, fetch, publish, *, identity=None, status=None, now=None):
    directory = Path(directory)
    with publication_lock(directory):
        lifecycle = None
        if identity is not None and identity.get("source") == "real":
            lifecycle = RealLifecycle(directory / "data")
            if not lifecycle.owner.exists():
                lifecycle.initialize()
            lifecycle.cleanup(now)
        if identity is not None:
            target = directory / "target.json"
            if target.exists():
                if json.loads(target.read_bytes()) != identity:
                    raise ValueError("Sync source or destination changed; reconcile into a separate directory")
            else:
                if any((directory / name).exists() for name in ("cursor.json", "pending.json", "batches")):
                    raise ValueError("Existing archive has no target identity; explicit reconciliation required")
                write_json(target, identity)
        state = directory / "cursor.json"
        after = json.loads(state.read_text())["cursor"] if state.exists() else 0
        if status is not None:
            check_source(directory, status(), after, expected_source=identity["source"])
        pending = directory / "pending.json"
        if pending.exists():
            batch = json.loads(pending.read_text())
            if lifecycle:
                if not batch["archive"].startswith("data/"):
                    raise ValueError("Legacy raw archive is not lifecycle managed")
                try:
                    lifecycle.readable(batch["archive"][5:], now)
                except (ValueError, FileNotFoundError):
                    record_gap(directory, "expired_pending_batch", after)
                    raise SourceGap("Pending payload expired; explicit recovery required") from None
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
                if identity and identity.get("source") and row["source"] != identity["source"]:
                    raise ValueError("Unexpected source; refusing to archive mixed data")
            archive = f"batches/{after + 1:020d}-{response['next_cursor']:020d}.json"
            if lifecycle:
                accepted = min(datetime.fromisoformat(r["accepted_at"].replace("Z", "+00:00")) for r in rows)
                lifecycle.register(archive, "raw", accepted.isoformat(), now=now)
                lifecycle.register(archive + ".manifest.json", "raw", accepted.isoformat(), now=now)
                archive = "data/" + archive
            write_json(directory / archive, response)
            batch = {"archive": archive, "sha256": digest((directory / archive).read_bytes()), "after": after}
            write_json(directory / (archive + ".manifest.json"), batch)
            write_json(pending, batch)
            if lifecycle:
                lifecycle.cleanup(now)
        for row in response["events"]:
            if row["source"] == "real":
                if status is None:
                    raise ValueError("Real sync requires authenticated source status")
                accepted = datetime.fromisoformat(row["accepted_at"].replace("Z", "+00:00"))
                current = now or datetime.now(UTC)
                if accepted.tzinfo is None or accepted + timedelta(days=7) <= current:
                    record_gap(directory, "expired_pending_batch", after)
                    raise SourceGap("Raw batch expired before publication; explicit recovery required")
            publish(row)  # Must return only after a durable sink acknowledgement.
        write_json(state, {"cursor": response["next_cursor"]})
        pending.unlink()
        return len(response["events"])


def kafka_sync(url, token, bootstrap, directory, *, lane=None, source=None, follow=False,
               poll_seconds=1.0, stop=None, on_batch=None):
    """Optional continuous reader; errors stop with its durable pending batch intact.

    Reuses connections between bounded polls. The caller/supervisor decides when
    to restart after errors; credentials never enter the target identity or logs.
    """
    from kafka import KafkaProducer
    if not token:
        raise ValueError("SNOW_READER_TOKEN required")
    endpoint = urlsplit(url)
    if endpoint.scheme not in {"http", "https"} or not endpoint.netloc or endpoint.username or endpoint.password or endpoint.query or endpoint.fragment:
        raise ValueError("Use an HTTP(S) service URL without credentials, query or fragment")
    if lane is not None and not re.fullmatch(r"[a-z0-9_]{1,24}", lane):
        raise ValueError("Invalid replay lane")
    if source not in {"real", "synthetic"}:
        raise ValueError("Invalid expected source")
    if not math.isfinite(poll_seconds) or not 0.1 <= poll_seconds <= 60:
        raise ValueError("poll_seconds must be 0.1..60")
    stop = stop or threading.Event()
    identity = dict(schema_version=1, url=url.rstrip("/"), bootstrap=bootstrap, lane=lane, source=source)
    producer = KafkaProducer(bootstrap_servers=bootstrap, acks="all", retries=3,
                             max_in_flight_requests_per_connection=1,
                             value_serializer=lambda value: json.dumps(value).encode())
    try:
        with httpx.Client(base_url=url, headers={"Authorization": "Bearer " + token}, timeout=15) as client:
            def status():
                response = client.get("/analytics/private/v1/status")
                response.raise_for_status()
                return response.json()
            def fetch(after):
                response = client.get("/analytics/private/v1/events", params={"after": after})
                if response.status_code == 410:
                    record_gap(directory, "http_410", after)
                    raise SourceGap("Collector retention gap (410); refusing automatic reset")
                response.raise_for_status()
                return response.json()
            def publish(row):
                if source is not None and row["source"] != source:
                    raise ValueError("Unexpected source; pending archive retained")
                event = row["event"]
                topic = f"snow.{row['source']}" + (f".{lane}" if lane else "") + ".events.v1"
                producer.send(topic,
                              key=f"{event['app']}:{event['event_id']}".encode(), value=row,
                              timestamp_ms=int(datetime.fromisoformat(row["accepted_at"].replace("Z", "+00:00")).timestamp() * 1000)).get(timeout=20)
            total = 0
            while not stop.is_set():
                count = sync_once(directory, fetch, publish, identity=identity,
                                  status=status if source == "real" else None)
                total += count
                if on_batch:
                    on_batch(count)
                if not follow or stop.wait(poll_seconds):
                    break
            return total
    finally:
        producer.close(timeout=10)
