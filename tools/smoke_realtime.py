"""Actual single-VM synthetic recovery acceptance. Never connects to production."""
import argparse
import json
import os
import subprocess
import time
from contextlib import contextmanager
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit
from uuid import NAMESPACE_URL, uuid5

import httpx
import pymysql
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
from vmware_lab import RUNTIME, capacity

from snow_statistics.io import digest, write_json
from snow_statistics.model import daily_metrics, deduplicate
from snow_statistics.realtime_fixture import phases

ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument("--host", default="192.168.216.133")
parser.add_argument("--lane", default="recovery01")
parser.add_argument("--action", choices=("init", "run", "restore", "inspect", "archive"), required=True)
args = parser.parse_args()
if not __import__("re").fullmatch(r"[a-z0-9_]{1,24}", args.lane):
    parser.error("A bounded new synthetic lane is required")
folder = ROOT / "runtime/realtime" / args.lane
folder.mkdir(parents=True, exist_ok=True)
database = "snow_realtime_" + args.lane
prefix = "snow.synthetic." + args.lane
ssh = ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "-o", "HostKeyAlias=snow-analysis",
       "-o", "ConnectTimeout=10", "-o", f"UserKnownHostsFile={RUNTIME / 'known_hosts'}", "-i", str(RUNTIME / "id_ed25519")]


def remote(command):
    return subprocess.check_output([*ssh, "snow@" + args.host, command], text=True, timeout=30)


def db():
    return pymysql.connect(host=args.host, port=9030, user="root", password="", autocommit=True, connect_timeout=5)


def query(sql):
    with db() as connection, connection.cursor(pymysql.cursors.DictCursor) as cursor:
        cursor.execute(sql)
        return cursor.fetchall()


def rows():
    return query(f"SELECT * FROM {database}.events_realtime ORDER BY app,business_key")


def metrics():
    return [dict(source=r["source"], app=r["app"], date=str(r["business_date"]), **{k: int(r[k]) for k in ("pv", "uv", "requests", "successes")})
            for r in query(f"SELECT * FROM {database}.daily_realtime ORDER BY source,business_date,app")]


def until(check, description, seconds=180):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
        except (httpx.HTTPError, pymysql.MySQLError, KeyError) as error:
            last = type(error).__name__
        time.sleep(1)
    raise RuntimeError(description + " timed out; last=" + str(last))


@contextmanager
def api():
    process = subprocess.Popen([*ssh, "-o", "ExitOnForwardFailure=yes", "-N", "-L", "127.0.0.1:18081:127.0.0.1:8081", "snow@" + args.host],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                               creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
    try:
        with httpx.Client(base_url="http://127.0.0.1:18081", timeout=30) as client:
            until(lambda: process.poll() is None and client.get("/overview").is_success, "Flink tunnel", 30)
            yield client
    finally:
        process.terminate()
        process.wait(timeout=10)


def send(batch):
    producer = KafkaProducer(bootstrap_servers=args.host + ":9092", acks="all", retries=2,
                             value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode())
    try:
        for row in batch:
            producer.send(prefix + ".events.v1", partition=0, value=row).get(timeout=10)
    finally:
        producer.close(timeout=10)


def archive(suffix):
    consumer = KafkaConsumer(bootstrap_servers=args.host + ":9092", enable_auto_commit=False,
                             value_deserializer=lambda raw: json.loads(raw))
    try:
        part = TopicPartition(prefix + "." + suffix + ".v1", 0)
        consumer.assign([part])
        begin, end = consumer.beginning_offsets([part])[part], consumer.end_offsets([part])[part]
        consumer.seek(part, begin)
        result = []
        deadline = time.monotonic() + 30
        while consumer.position(part) < end:
            if time.monotonic() > deadline:
                raise RuntimeError("Kafka bounded archive timed out")
            for records in consumer.poll(timeout_ms=500).values():
                result.extend(dict(offset=r.offset, value=r.value) for r in records if r.offset < end)
        write_json(folder / (suffix + ".json"), dict(topic=part.topic, begin=begin, end=end, rows=result))
        return result
    finally:
        consumer.close()


print(json.dumps(capacity(1024)), flush=True)
if args.action == "init":
    until(lambda: any(r["Alive"] for r in query("SHOW BACKENDS")), "Doris readiness")
    admin = KafkaAdminClient(bootstrap_servers=args.host + ":9092", client_id="snow-realtime-acceptance")
    try:
        topics = [prefix + "." + s + ".v1" for s in ("events", "late", "duplicates", "quarantine")]
        existing = set(admin.list_topics())
        for suffix in ("events", "late", "duplicates", "quarantine"):
            if prefix + "." + suffix + ".v1" in existing and archive(suffix):
                raise ValueError("Use a new lane; existing topics and data are preserved")
        missing = [t for t in topics if t not in existing]
        if missing:
            admin.create_topics([NewTopic(t, 1, 1, topic_configs={"retention.ms": "-1", "segment.bytes": "16777216"}) for t in missing])
    finally:
        admin.close()
    with db() as connection, connection.cursor() as cursor:
        schema = (ROOT / "warehouse/doris/schema.sql").read_text().replace("snow.", database + ".").replace("EXISTS snow;", "EXISTS " + database + ";")
        for statement in schema.split(";"):
            if statement.strip():
                cursor.execute(statement)
    batches = phases(args.lane, datetime.now(UTC).isoformat())
    write_json(folder / "fixture.json", batches)
    print("Created four isolated topics and the synthetic Doris tables", flush=True)
elif args.action == "run":
    if (folder / "job.json").exists():
        raise ValueError("Existing job receipt; inspect or use a new lane")
    batches = json.loads((folder / "fixture.json").read_bytes())
    with api() as client:
        jar = ROOT / "warehouse/flink/target/snow-realtime-0.1.0.jar"
        with jar.open("rb") as stream:
            response = client.post("/jars/upload", files={"jarfile": (jar.name, stream, "application/java-archive")}, timeout=120)
        response.raise_for_status()
        jar_id = response.json()["filename"].rsplit("/", 1)[-1]
        response = client.post("/jars/" + jar_id + "/run", json={"entryClass": "dev.xiaob.snow.RealtimeJob", "parallelism": 1})
        if not response.is_success:
            write_json(folder / "submission-error.json", response.json())
        response.raise_for_status()
        job = response.json()["jobid"]
        write_json(folder / "job.json", dict(job_id=job, jar_id=jar_id, jar_sha256=digest(jar.read_bytes())))
        def get(path):
            response = client.get("/jobs/" + job + path)
            response.raise_for_status()
            return response.json()
        until(lambda: get("")["state"] == "RUNNING", "job running")
        send(batches[0])
        until(lambda: len(rows()) == 4, "first committed facts")
        before = get("/checkpoints")
        assert urlsplit(before["latest"]["completed"]["external_path"]).path.startswith("/checkpoints/")
        write_json(folder / "checkpoint-before.json", before)
        print("First four facts committed; killing only the project TaskManager", flush=True)
        remote("sudo docker kill snow-lab-realtime-taskmanager-1")
        send(batches[1])
        remote("sudo docker start snow-lab-realtime-taskmanager-1")
        until(lambda: get("")["state"] == "RUNNING", "job recovery")
        until(lambda: len(rows()) == 6, "recovered committed facts")
        restored = get("/checkpoints")
        assert restored["counts"]["restored"] >= 1 and restored["latest"]["restored"]["id"] >= before["latest"]["completed"]["id"]
        write_json(folder / "checkpoint-restored.json", restored)
        # Wait for the periodic 30-second watermark after the 16:15 event.
        time.sleep(2)
        send(batches[2])
        until(lambda: len(rows()) == 9, "late-correctable facts")
        time.sleep(2)
        actual = rows()
        assert len({(r["source"], r["app"], r["business_key"]) for r in actual}) == 9
        request = next(r for r in actual if r["business_key"] == "request:req_A")
        assert str(request["business_date"]) == "2026-01-01" and request["success"] == 1
        all_input = [r for batch in batches for r in batch]
        valid, quality, _ = deduplicate([r for r in all_input if r["source"] == "synthetic"])
        expected_live = daily_metrics([r for r in valid if r["seq"] not in {12, 14}])
        assert metrics() == expected_live
        outputs = {s: archive(s) for s in ("events", "late", "duplicates", "quarantine")}
        assert {r["value"]["seq"] for r in outputs["late"]} == {12, 14}
        assert {r["value"]["seq"] for r in outputs["duplicates"]} == {5, 6, 15}
        assert len(outputs["quarantine"]) == 3
        final = dict(engine="Flink 1.20.3 / Kafka 3.9.1 / Doris 3.0.6.2", source="synthetic", lane=args.lane,
                     job_id=job, input_rows=17, live_rows=9, late_rows=2, duplicate_rows=3, quarantine_rows=3,
                     oracle_quality_synthetic=quality, wrong_source_rejected=1, checkpoint_restore_verified=True,
                     first_request_day_and_success_preserved=True, live_metrics=expected_live,
                     offline_corrected_metrics=daily_metrics(valid),
                     side_output_delivery="at_least_once; raw diagnostic counts apply to this observed run",
                     checkpoint_final=get("/checkpoints"))
        write_json(folder / "receipt.json", final)
        print(json.dumps({k: final[k] for k in ("input_rows", "live_rows", "late_rows", "duplicate_rows", "quarantine_rows", "checkpoint_restore_verified")}), flush=True)
elif args.action == "restore":
    if (folder / "restored-job.json").exists():
        raise ValueError("Restore evidence exists; inspect it before another experiment")
    previous = json.loads((folder / "job.json").read_bytes())
    receipt = json.loads((folder / "receipt.json").read_bytes())
    batches = json.loads((folder / "fixture.json").read_bytes())
    with api() as client:
        source_path = folder / "session-restore-source.json"
        if source_path.exists():
            checkpoint = json.loads(source_path.read_bytes())
            assert not any(j["state"] == "RUNNING" for j in client.get("/jobs/overview").json()["jobs"])
        else:
            checkpoint = client.get("/jobs/" + previous["job_id"] + "/checkpoints").json()["latest"]["completed"]
            write_json(source_path, checkpoint)
            response = client.patch("/jobs/" + previous["job_id"], params={"mode": "cancel"})
            response.raise_for_status()
            until(lambda: client.get("/jobs/" + previous["job_id"]).json()["state"] == "CANCELED", "job cancellation", 60)
            remote("sudo docker stop snow-lab-realtime-taskmanager-1 snow-lab-realtime-jobmanager-1")
            remote("sudo docker start snow-lab-realtime-jobmanager-1 snow-lab-realtime-taskmanager-1")
            until(lambda: client.get("/overview").is_success, "session restart", 60)
        path = checkpoint["external_path"]
        jar = ROOT / "warehouse/flink/target/snow-realtime-0.1.0.jar"
        assert digest(jar.read_bytes()) == previous["jar_sha256"], "Restore requires the tested artifact"
        with jar.open("rb") as stream:
            response = client.post("/jars/upload", files={"jarfile": (jar.name, stream, "application/java-archive")}, timeout=120)
        response.raise_for_status()
        jar_id = response.json()["filename"].rsplit("/", 1)[-1]
        response = client.post("/jars/" + jar_id + "/run", json={"entryClass": "dev.xiaob.snow.RealtimeJob", "parallelism": 1,
                                                                            "savepointPath": path, "allowNonRestoredState": False})
        if not response.is_success:
            write_json(folder / "restore-error.json", response.json())
        response.raise_for_status()
        job = response.json()["jobid"]
        write_json(folder / "restored-job.json", dict(job_id=job, checkpoint=path, jar_sha256=previous["jar_sha256"]))
        until(lambda: client.get("/jobs/" + job).json()["state"] == "RUNNING", "restored session job")
        duplicate, request, new_page = deepcopy(batches[0][0]), deepcopy(batches[0][2]), deepcopy(batches[0][0])
        duplicate["seq"] = 18
        request["seq"] = 19
        request["event"].update(event_id=str(uuid5(NAMESPACE_URL, args.lane + "/restore/19")), occurred_at="2026-01-01T16:00:07Z")
        new_page["seq"] = 20
        new_page["event"].update(event_id=str(uuid5(NAMESPACE_URL, args.lane + "/restore/20")), occurred_at="2026-01-01T16:15:01Z")
        extra = [duplicate, request, new_page]
        write_json(folder / "restore-fixture.json", extra)
        send(extra)
        until(lambda: len(rows()) == 10, "post-restore facts")
        checkpoints = client.get("/jobs/" + job + "/checkpoints").json()
        assert checkpoints["latest"]["restored"]["external_path"] == path
        valid, quality, _ = deduplicate([r for r in sum(batches, []) + extra if r["source"] == "synthetic"])
        live = daily_metrics([r for r in valid if r["seq"] not in {12, 14}])
        assert metrics() == live
        for suffix in ("events", "late", "duplicates", "quarantine"):
            archive(suffix)
        write_json(folder / "session-restore.json", dict(checkpoint_source=checkpoint, restored_job_id=job, checkpoints=checkpoints,
                   live_rows=10, extra_events=3, event_and_request_state_retained=True, live_metrics=live,
                   oracle_quality_synthetic=quality, offline_corrected_metrics=daily_metrics(valid)))
        print("JobManager/TaskManager session restarted; checkpoint source, both dedup states and ten facts verified", flush=True)
elif args.action == "inspect":
    with api() as client:
        jobs = client.get("/jobs/overview").json()
        print(json.dumps(jobs))
        for job in jobs.get("jobs", []):
            for suffix in ("exceptions", "checkpoints"):
                result = client.get("/jobs/" + job["jid"] + "/" + suffix).json()
                write_json(folder / ("inspect-" + suffix + ".json"), result)
                print(json.dumps(result)[:12000])
    print(json.dumps(dict(rows=len(rows()), metrics=metrics())))
else:
    for suffix in ("events", "late", "duplicates", "quarantine"):
        print(suffix, len(archive(suffix)))
