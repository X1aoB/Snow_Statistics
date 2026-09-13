"""Isolated synthetic fixtures exercising the real code branch, never production.

Run capture with only control Kafka, land with HDFS/YARN, then ack with Kafka.
Every lane must start fixture_ and use its own application-managed ODS root.
"""
import argparse
import json
import re
import secrets
import socket
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import uvicorn

from snow_statistics.api import create_app
from snow_statistics.config import Settings
from snow_statistics.hdfs_landing import HdfsSink
from snow_statistics.io import write_json
from snow_statistics.kafka_landing import KafkaSource
from snow_statistics.landing import acknowledge, capture, land
from snow_statistics.model import build
from snow_statistics.simulator import generate
from snow_statistics.sync import kafka_sync


def capture_fixture(directory, lane, bootstrap):
    from kafka import KafkaAdminClient
    from kafka.admin import NewTopic
    topic = "snow.real." + lane + ".events.v1"
    if (directory / "fixture-receipt.json").exists():
        raise ValueError("A completed fixture run must use a fresh lane")
    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    try:
        if topic in admin.list_topics():
            raise ValueError("Fixture topic already exists; preserve prior attempt and choose a new lane")
        admin.create_topics([NewTopic(topic, 1, 1, topic_configs={"retention.ms": "604800000", "segment.ms": "60000"})])
    finally:
        admin.close()
    fixture = generate(users=12)
    now = datetime.now(UTC)
    shift = datetime.combine(now.date() - timedelta(days=4), datetime.min.time(), UTC) - datetime(2026, 1, 1, tzinfo=UTC)
    for row in fixture["events"]:
        row["source"] = "real"  # Declared only inside this isolated synthetic acceptance.
        event = row["event"]
        event["occurred_at"] = (datetime.fromisoformat(event["occurred_at"].replace("Z", "+00:00")) + shift).isoformat()
        row["accepted_at"] = now.isoformat()
    oracle = build(fixture)["daily"]
    token, server_token = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
    settings = Settings(mode="full", source="real", db=directory / "collector/statistics.db",
                        reader_token=token, server_token=server_token, aggregate_interval=0.1,
                        allowed_paths=frozenset(r["event"]["path"] for r in fixture["events"] if "path" in r["event"]),
                        allowed_characters=frozenset(r["event"]["character_id"] for r in fixture["events"] if "character_id" in r["event"]))
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    port = listener.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(create_app(settings), access_log=False, log_level="error"))
    worker = threading.Thread(target=server.run, kwargs={"sockets": [listener]}, daemon=True)
    worker.start()
    try:
        with httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=5, trust_env=False) as client:
            deadline = time.monotonic() + 15
            while not server.started:
                if time.monotonic() > deadline:
                    raise TimeoutError("Fixture collector failed to start")
                time.sleep(0.05)
            for start in range(0, len(fixture["events"]), 10):
                batch = [row["event"] for row in fixture["events"][start:start + 10]]
                response = client.post("/analytics/v1/events", json={"events": batch},
                                       headers={"Authorization": "Bearer " + server_token, "Origin": "https://xiaob.dev"})
                response.raise_for_status()
            synced = kafka_sync(str(client.base_url).rstrip("/"), token, bootstrap, directory / "sync", lane=lane, source="real")
            deadline = time.monotonic() + 15
            while True:
                exact = client.get("/analytics/private/v1/summary.json", headers={"Authorization": "Bearer " + token}).json()
                actual = [{k: r[k] for k in ("app", "date", "pv", "uv", "requests", "successes")} | {"source": "real"} for r in exact["daily"]]
                if sorted(actual, key=lambda r: (r["date"], r["app"])) == sorted(oracle, key=lambda r: (r["date"], r["app"])):
                    break
                if time.monotonic() > deadline:
                    raise ValueError("Independent lightweight metric reconciliation failed")
                time.sleep(0.1)
            write_json(directory / "expected.json", {"daily": oracle})
    finally:
        server.should_exit = True
        worker.join(15)
        listener.close()
        if worker.is_alive():
            raise TimeoutError("Fixture collector did not stop")
    broker = KafkaSource(bootstrap, "snow-ods-" + lane.replace("_", "-"), source="real", event_lane=lane)
    try:
        batch = capture(directory / "ods", broker, provenance="real", event_lane=lane)
    finally:
        broker.close()
    receipt = dict(input_origin="synthetic fixtures", exercised_branch="real", production_requests=0,
                   topic=topic, synced=synced, batch_id=batch, lite_equal=True,
                   date_from=min(r["date"] for r in oracle), date_to=max(r["date"] for r in oracle),
                   cutoff=datetime.now(UTC).isoformat())
    write_json(directory / "fixture-receipt.json", receipt)
    return receipt


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("capture", "land", "ack"))
    parser.add_argument("--lane", required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"fixture_[a-z0-9_]{1,16}", args.lane):
        parser.error("Only new fixture_* lanes are allowed; never a real production lane")
    directory = Path("runtime/production-acceptance") / args.lane
    directory.mkdir(parents=True, exist_ok=True)
    env = dict(line.split("=", 1) for line in Path("lab/.env").read_text().splitlines() if "=" in line)
    if args.phase == "capture":
        result = capture_fixture(directory, args.lane, env["CONTROL_IP"] + ":9092")
    elif args.phase == "land":
        sink = HdfsSink(env["CONTROL_IP"], args.lane.replace("_", "-"),
                        {"snow-compute": env["COMPUTE_IP"], "snow-analysis": env["ANALYSIS_IP"]}, source="real")
        try:
            result = land(directory / "ods", sink)
        finally:
            sink.client.close()
    else:
        broker = KafkaSource(env["CONTROL_IP"] + ":9092", "snow-ods-" + args.lane.replace("_", "-"), source="real", event_lane=args.lane)
        try:
            result = acknowledge(directory / "ods", broker)
        finally:
            broker.close()
    print(json.dumps(result))


if __name__ == "__main__":
    main()
