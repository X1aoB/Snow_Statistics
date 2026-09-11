"""Synthetic Kafka/CDC connectivity evidence, with no credential output."""
import json
import os
from pathlib import Path

import httpx
import pymysql
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

from snow_statistics.simulator import generate

for line in Path("lab/.env").read_text().splitlines():
    key, value = line.split("=", 1)
    os.environ[key] = value
host = os.environ["CONTROL_IP"]
admin = KafkaAdminClient(bootstrap_servers=host + ":9092", request_timeout_ms=15000)
for source in ("real", "synthetic"):
    for suffix in ("events", "late", "quarantine"):
        try:
            admin.create_topics([NewTopic(f"snow.{source}.{suffix}.v1", 1, 1)])
        except TopicAlreadyExistsError:
            pass
admin.close()
producer = KafkaProducer(bootstrap_servers=host + ":9092", acks="all", value_serializer=lambda value: json.dumps(value).encode())
rows = generate(users=2)["events"]
for row in rows:
    producer.send("snow.synthetic.events.v1", value=row).get(timeout=20)
producer.close()
consumer = KafkaConsumer("snow.synthetic.events.v1", bootstrap_servers=host + ":9092", group_id=None,
                         auto_offset_reset="earliest", consumer_timeout_ms=5000, enable_auto_commit=False)
count = 0
for record in consumer:
    assert json.loads(record.value)["source"] == "synthetic"
    count += 1
consumer.close()
with pymysql.connect(host="127.0.0.1", user="snow_simulator", password=os.environ["LAB_MYSQL_PASSWORD"], database="snow_ops") as db:
    with db.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM tickets")
        tickets = cursor.fetchone()[0]
response = httpx.get("http://127.0.0.1:8083/connectors/snow-synthetic-ops/status", timeout=10)
response.raise_for_status()
status = response.json()
receipt = dict(kafka_records=count, sample_records=len(rows), mysql_tickets=tickets,
               connector=status["connector"]["state"], tasks=[t["state"] for t in status["tasks"]])
Path("runtime").mkdir(exist_ok=True)
Path("runtime/ingest-receipt.json").write_text(json.dumps(receipt, indent=2))
print(json.dumps(receipt))
assert count >= len(rows) and tickets > 0 and receipt["connector"] == "RUNNING" and receipt["tasks"] == ["RUNNING"]
