"""Bounded 100k historical Kafka/Flink/Doris reconciliation in one new synthetic lane."""

import argparse
import gzip
import hashlib
import json
import os
import re
import socket
import subprocess
import time
from contextlib import contextmanager
from datetime import datetime

import httpx
import pymysql
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from vmware_lab import (
    MAX_PROJECT_BYTES,
    MIN_HOST_AVAILABLE_MIB,
    ROOT,
    RUNTIME,
    VMWARE,
    capacity,
    guest_ip,
    run,
)

from snow_statistics.contracts import business_day
from snow_statistics.io import digest, write_json
from snow_statistics.model import deduplicate

parser = argparse.ArgumentParser()
parser.add_argument("--lane", required=True)
args = parser.parse_args()
if not re.fullmatch(r"[a-z0-9_]{1,24}", args.lane):
    parser.error("A bounded new synthetic lane is required")
folder = ROOT / "runtime/realtime-scale" / args.lane
if (folder / "job.json").exists() or (folder / "producer.json").exists():
    parser.error("Existing run is preserved; inspect its receipts before another run")
manifest = json.loads((folder / "manifest.json").read_bytes())
expected = json.loads((folder / "expected.json").read_bytes())
assert manifest["events"] == 100000 and manifest["source"] == "synthetic"
assert digest((folder / "events.jsonl.gz").read_bytes()) == manifest["replay_sha256"]
assert json.loads((folder / "lite-receipt.json").read_bytes())["daily"] == expected["daily"]
host = guest_ip(VMWARE / "vmrun.exe", RUNTIME / "snow-analysis/snow-analysis.vmx")
running = run(VMWARE / "vmrun.exe", "-T", "ws", "list")
assert "Total running VMs: 1" in running and "snow-analysis.vmx" in running
prefix, database = "snow.synthetic." + args.lane, "snow_realtime_" + args.lane
ssh = [
    "ssh",
    "-o",
    "BatchMode=yes",
    "-o",
    "ConnectTimeout=10",
    "-o",
    "StrictHostKeyChecking=yes",
    "-o",
    "HostKeyAlias=snow-analysis",
    "-o",
    f"UserKnownHostsFile={RUNTIME / 'known_hosts'}",
    "-i",
    str(RUNTIME / "id_ed25519"),
]
samples, last_resource_check = [], 0


def remote(command):
    return subprocess.check_output([*ssh, "snow@" + host, command], text=True, timeout=60)


def check():
    global last_resource_check
    if time.monotonic() - last_resource_check < 5:
        return
    sample = capacity()
    sample["host_available_mib"] = (
        int(
            run(
                "powershell",
                "-NoProfile",
                "-Command",
                "(Get-CimInstance Win32_OperatingSystem).FreePhysicalMemory",
            )
        )
        // 1024
    )
    sample["measured_epoch"] = time.time()
    samples.append(sample)
    last_resource_check = time.monotonic()
    write_json(folder / "resources.json", samples)
    if (
        sample["project_files_gib"] >= MAX_PROJECT_BYTES / 1024**3 - 0.25
        or sample["host_available_mib"] < MIN_HOST_AVAILABLE_MIB
    ):
        raise RuntimeError("Resource early-stop boundary; preserve history")


def connection(streaming=False):
    return pymysql.connect(
        host=host,
        port=9030,
        user="root",
        password="",
        autocommit=True,
        connect_timeout=5,
        read_timeout=30,
        cursorclass=pymysql.cursors.SSDictCursor if streaming else pymysql.cursors.DictCursor,
    )


def query(sql):
    with connection() as db, db.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchall()


def observe(label):
    script = (ROOT / "tools/observe_realtime_memory.sh").read_text(encoding="utf-8")
    output = subprocess.check_output(
        [*ssh, "snow@" + host, "bash -se"], input=script.replace("\r\n", "\n").encode(), timeout=60
    )
    write_json(folder / ("memory-" + label + ".json"), json.loads(output))


def until(test, label, seconds=300):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        check()
        value = test()
        if value:
            return value
        time.sleep(1)
    raise RuntimeError(label + " deadline; retained state requires inspection")


@contextmanager
def flink():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    process = subprocess.Popen(
        [
            *ssh,
            "-o",
            "ExitOnForwardFailure=yes",
            "-N",
            "-L",
            f"127.0.0.1:{port}:127.0.0.1:8081",
            "snow@" + host,
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
    )
    try:
        with httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=30) as client:

            def ready():
                try:
                    return process.poll() is None and client.get("/overview").is_success
                except httpx.HTTPError:
                    return False

            until(ready, "Flink tunnel", 30)
            yield client
    finally:
        process.terminate()
        process.wait(timeout=10)


def archive(suffix, expected_count):
    consumer = KafkaConsumer(bootstrap_servers=host + ":9092", enable_auto_commit=False)
    try:
        part = TopicPartition(prefix + "." + suffix + ".v1", 0)
        consumer.assign([part])
        begin, end = consumer.beginning_offsets([part])[part], consumer.end_offsets([part])[part]
        assert begin == 0 and end == expected_count, (suffix, begin, end, expected_count)
        consumer.seek(part, begin)
        hashed, count = hashlib.sha256(), 0
        deadline = time.monotonic() + 180
        target = folder / (suffix + f"-{expected_count}.jsonl.gz")
        with target.open("wb") as raw, gzip.GzipFile(filename="", fileobj=raw, mode="wb", mtime=0) as out:
            while consumer.position(part) < end:
                check()
                if time.monotonic() > deadline:
                    raise RuntimeError("Kafka archive deadline")
                for records in consumer.poll(timeout_ms=500, max_records=2000).values():
                    for record in records:
                        if record.offset < end:
                            assert record.offset == count
                            out.write(record.value + b"\n")
                            hashed.update(record.value + b"\n")
                            count += 1
        return dict(
            topic=part.topic,
            begin=begin,
            end=end,
            records=count,
            wire_sha256=hashed.hexdigest(),
            gzip_sha256=digest(target.read_bytes()),
        )
    finally:
        consumer.close()


def produce(records, begin):
    producer = KafkaProducer(
        bootstrap_servers=host + ":9092",
        acks="all",
        retries=2,
        max_in_flight_requests_per_connection=1,
        linger_ms=5,
        compression_type="gzip",
    )
    hashed, count, pending = hashlib.sha256(), 0, []
    started = time.monotonic()
    try:
        for raw in records:
            data = raw.strip()
            hashed.update(data + b"\n")
            pending.append(producer.send(prefix + ".events.v1", partition=0, value=data))
            if len(pending) == 1000:
                for future in pending:
                    assert future.get(timeout=15).offset == begin + count
                    count += 1
                pending = []
                check()
        for future in pending:
            assert future.get(timeout=15).offset == begin + count
            count += 1
    finally:
        producer.close(timeout=15)
    return dict(
        records=count, wire_sha256=hashed.hexdigest(), elapsed_seconds=round(time.monotonic() - started, 3)
    )


def metrics():
    return [
        dict(
            source=row["source"],
            app=row["app"],
            date=str(row["business_date"]),
            **{key: int(row[key]) for key in ("pv", "uv", "requests", "successes")},
        )
        for row in query(f"SELECT * FROM {database}.daily_realtime ORDER BY source,business_date,app")
    ]


def fact_digest():
    columns = "source,app,business_key,event_type,business_date,anonymous_id,page,character_id,success,event_time,accepted_at,business_version"
    sha = hashlib.sha256()
    count = 0
    with connection(streaming=True) as db, db.cursor() as cursor:
        cursor.execute(f"SELECT {columns} FROM {database}.events_realtime ORDER BY app,business_key")
        for row in cursor:
            row["business_date"] = str(row["business_date"])
            for key in ("event_time", "accepted_at"):
                row[key] = row[key].strftime("%Y-%m-%d %H:%M:%S.%f")
            if row["success"] is not None:
                row["success"] = int(row["success"])
            sha.update((json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n").encode())
            count += 1
    return dict(rows=count, sha256=sha.hexdigest())


def expected_facts():
    with gzip.open(folder / "events.jsonl.gz", "rt", encoding="utf-8") as stream:
        valid, quality, bad = deduplicate([json.loads(line) for line in stream])
    assert not bad and quality["valid"] == 90000
    rows = []
    for row in valid:
        event = row["event"]
        kind = event["event_type"]
        at = datetime.fromisoformat(event["occurred_at"].replace("Z", "+00:00"))
        rows.append(
            dict(
                source="synthetic",
                app=event["app"],
                business_key=(
                    "request:" + event["request_id"]
                    if kind == "request_complete"
                    else "event:" + event["event_id"]
                ),
                event_type=kind,
                business_date=business_day(at),
                anonymous_id=event.get("anonymous_id"),
                page=event.get("path"),
                character_id=event.get("character_id"),
                success=int(event["success"]) if kind == "request_complete" else None,
                business_version=2**63 - 1 - row["seq"],
                event_time=at.strftime("%Y-%m-%d %H:%M:%S.%f"),
                accepted_at=datetime.fromisoformat(row["accepted_at"].replace("Z", "+00:00")).strftime(
                    "%Y-%m-%d %H:%M:%S.%f"
                ),
            )
        )
    sha = hashlib.sha256()
    for row in sorted(rows, key=lambda r: (r["app"], r["business_key"])):
        sha.update((json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n").encode())
    return dict(rows=len(rows), sha256=sha.hexdigest())


def main():
    write_json(folder / "preflight.json", capacity(1024))
    check()
    # Only known safe fields are returned; never print the full container environment.
    assert (
        remote("sudo docker exec snow-lab-realtime-jobmanager-1 printenv SNOW_REPLAY_LANE").strip()
        == args.lane
    )
    assert (
        remote("sudo docker exec snow-lab-realtime-jobmanager-1 printenv DORIS_TABLE").strip()
        == database + ".events_realtime"
    )

    def doris_ready():
        try:
            return any(row["Alive"] for row in query("SHOW BACKENDS"))
        except pymysql.OperationalError:
            return False

    until(doris_ready, "Doris backend ready")
    with flink():
        pass
    assert query(f"SELECT COUNT(*) AS n FROM {database}.events_realtime")[0]["n"] == 0
    archive("events", 0)
    observe("before")
    reference = expected_facts()
    write_json(folder / "expected-facts.json", reference)
    jar = ROOT / "warehouse/flink/target/snow-realtime-0.1.0.jar"
    assert (
        digest(jar.read_bytes())
        == json.loads((ROOT / "runtime/freshness/freshness01/receipt.json").read_bytes())["jar_sha256"]
    )
    with gzip.open(folder / "events.jsonl.gz", "rb") as stream:
        sent = produce(stream, 0)
    write_json(folder / "producer.json", sent)
    original = archive("events", 100000)
    assert original["wire_sha256"] == sent["wire_sha256"]
    with flink() as client:
        assert not any(
            j["state"] not in ("FINISHED", "CANCELED", "FAILED")
            for j in client.get("/jobs/overview").json()["jobs"]
        )
        with jar.open("rb") as stream:
            response = client.post(
                "/jars/upload", files={"jarfile": (jar.name, stream, "application/java-archive")}, timeout=120
            )
        response.raise_for_status()
        jar_id = response.json()["filename"].rsplit("/", 1)[-1]
        started = time.monotonic()
        response = client.post(
            "/jars/" + jar_id + "/run", json={"entryClass": "dev.xiaob.snow.RealtimeJob", "parallelism": 1}
        )
        response.raise_for_status()
        job = response.json()["jobid"]
        write_json(folder / "job.json", dict(job_id=job, jar_sha256=digest(jar.read_bytes())))

        def get(suffix=""):
            response = client.get("/jobs/" + job + suffix)
            response.raise_for_status()
            return response.json()

        until(lambda: get()["state"] == "RUNNING", "Flink running")

        def facts_ready():
            state = get()["state"]
            if state in ("FAILED", "CANCELED"):
                write_json(folder / "exceptions.json", get("/exceptions"))
                raise RuntimeError("Flink terminated")
            return query(f"SELECT COUNT(*) AS n FROM {database}.events_realtime")[0]["n"] == 90000

        until(facts_ready, "90k committed facts", 600)
        elapsed = round(time.monotonic() - started, 3)
        actual = fact_digest()
        assert actual == reference
        assert metrics() == expected["daily"]
        before = get("/checkpoints")
        # Wait for a later successful checkpoint after the full result is visible.
        checkpoint_id = before["latest"]["completed"]["id"]
        until(
            lambda: get("/checkpoints")["latest"]["completed"]["id"] > checkpoint_id, "full-state checkpoint"
        )
        before = get("/checkpoints")
        write_json(folder / "checkpoint-before.json", before)
        observe("before-recovery")
        for suffix, n in (("late", 0), ("quarantine", 0), ("duplicates", 10000)):
            archive(suffix, n)
        print("100k history -> 90k exact facts and 14 daily rows; restoring TaskManager", flush=True)
        remote("sudo docker kill snow-lab-realtime-taskmanager-1")
        remote("sudo docker start snow-lab-realtime-taskmanager-1")
        until(
            lambda: get()["state"] == "RUNNING" and get("/checkpoints")["counts"]["restored"] >= 1,
            "TaskManager checkpoint recovery",
            180,
        )
        restored = get("/checkpoints")
        write_json(folder / "checkpoint-restored.json", restored)
        assert restored["latest"]["restored"]["id"] >= before["latest"]["completed"]["id"]
        with gzip.open(folder / "events.jsonl.gz", "rb") as stream:
            replay = [next(stream) for _ in range(1000)]
        resent = produce(replay, 100000)
        write_json(folder / "replay-producer.json", resent)
        # New Kafka positions must reach a successful checkpoint after recovery.
        checkpoint_id = get("/checkpoints")["latest"]["completed"]["id"]
        until(lambda: get("/checkpoints")["latest"]["completed"]["id"] > checkpoint_id, "replay checkpoint")
        consumer = KafkaConsumer(bootstrap_servers=host + ":9092", enable_auto_commit=False)
        part = TopicPartition(prefix + ".duplicates.v1", 0)
        try:
            until(lambda: consumer.end_offsets([part])[part] >= 11000, "all replay duplicate diagnostics")
        finally:
            consumer.close()
        assert fact_digest() == actual and metrics() == expected["daily"]
        archives = {
            suffix: archive(suffix, n)
            for suffix, n in (("events", 101000), ("late", 0), ("quarantine", 0), ("duplicates", 11000))
        }
        result = dict(
            source="synthetic",
            lane=args.lane,
            job_id=job,
            jar_sha256=digest(jar.read_bytes()),
            input_events=100000,
            valid_facts=90000,
            initial_duplicates=10000,
            recovery_replay_events=1000,
            fact_digest=actual,
            daily=expected["daily"],
            lite_yarn_doris_equal=True,
            checkpoint_recovery_verified=True,
            checkpoint_final=get("/checkpoints"),
            submit_to_all_facts_observed_seconds=elapsed,
            producer=sent,
            archives=archives,
            scope="100k preloaded history in event-time order on one analysis VM; not production freshness, source arrival disorder or sustained throughput",
        )
        write_json(folder / "receipt.json", result)
        observe("after-recovery")
        print(
            json.dumps(
                {
                    k: result[k]
                    for k in (
                        "valid_facts",
                        "recovery_replay_events",
                        "checkpoint_recovery_verified",
                        "submit_to_all_facts_observed_seconds",
                    )
                }
            ),
            flush=True,
        )


try:
    main()
finally:
    # Always attempt both operations. A failed cancellation must not skip VM shutdown.
    errors = []
    try:
        remote("bash /home/snow/Snow_Statistics/tools/stop_realtime_node.sh")
    except Exception as error:
        errors.append(type(error).__name__)
    try:
        run(VMWARE / "vmrun.exe", "-T", "ws", "stop", RUNTIME / "snow-analysis/snow-analysis.vmx", "soft")
    except Exception as error:
        errors.append(type(error).__name__)
    write_json(folder / "shutdown.json", dict(errors=errors, resources=capacity()))
    if errors:
        raise RuntimeError("Inspect shutdown errors; do not assume services are stopped")
