"""Actual bounded HTTP -> archived Kafka -> Flink -> Doris lab, then VM-off lite.

Synthetic only. Reuses the NAT analysis services provisioned with smoke_realtime
init, never publishes to a business endpoint. Keeps all local artifacts on error.
"""
import argparse
import json
import secrets
import socket
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

import httpx
import pymysql
import uvicorn
from vmware_lab import RUNTIME, VMWARE, capacity, guest_ip, run

from snow_statistics.api import create_app
from snow_statistics.config import Settings
from snow_statistics.freshness import summarize
from snow_statistics.io import digest, write_json
from snow_statistics.model import daily_metrics
from snow_statistics.store import Store
from snow_statistics.sync import kafka_sync

ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument("--lane", required=True)
args = parser.parse_args()
if not __import__("re").fullmatch(r"[a-z0-9_]{1,24}", args.lane):
    parser.error("Bounded synthetic lane required")
host = guest_ip(VMWARE / "vmrun.exe", RUNTIME / "snow-analysis/snow-analysis.vmx")
running = run(VMWARE / "vmrun.exe", "-T", "ws", "list")
assert "Total running VMs: 1" in running and "snow-analysis.vmx" in running
folder = ROOT / "runtime/freshness" / args.lane
if folder.exists():
    raise ValueError("Use a new lane; existing receipts and collector data are preserved")
folder.mkdir(parents=True)
write_json(folder / "resources-before.json", capacity(1024))
database = "snow_realtime_" + args.lane
ssh = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "StrictHostKeyChecking=yes",
       "-o", "HostKeyAlias=snow-analysis", "-o", f"UserKnownHostsFile={RUNTIME / 'known_hosts'}",
       "-i", str(RUNTIME / "id_ed25519")]


def query(sql):
    with pymysql.connect(host=host, port=9030, user="root", password="", connect_timeout=5,
                         read_timeout=10, autocommit=True, cursorclass=pymysql.cursors.DictCursor) as db, db.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchall()


def until(check, seconds=90):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = check()
        if value:
            return value
        time.sleep(.5)
    raise RuntimeError("Bounded acceptance deadline; artifacts retained")


@contextmanager
def flink():
    process = subprocess.Popen([*ssh, "-o", "ExitOnForwardFailure=yes", "-N", "-L",
                                "127.0.0.1:18081:127.0.0.1:8081", "snow@" + host],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                               creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0)
    try:
        with httpx.Client(base_url="http://127.0.0.1:18081", timeout=30) as client:
            def ready():
                if process.poll() is not None:
                    raise RuntimeError("SSH tunnel exited")
                try:
                    return client.get("/overview").json().get("taskmanagers") == 1
                except httpx.HTTPError:
                    return False
            until(ready)
            yield client
    finally:
        process.terminate()
        process.wait(timeout=10)


def wave(index):
    def identifier(name):
        return str(uuid5(NAMESPACE_URL, args.lane + "/" + name))
    base = dict(schema_version=1, occurred_at=datetime.now(UTC).isoformat())
    visitor_web, visitor_snow = identifier(f"web/{index % 12}"), identifier(f"snow/{index % 10}")
    rows = [dict(app="mywebsite", event_type="page_view", path="/", anonymous_id=visitor_web),
            dict(app="project_snow", event_type="page_view", path="/", anonymous_id=visitor_snow),
            dict(app="project_snow", event_type="character_select", character_id="sample_character", anonymous_id=visitor_snow),
            dict(app="project_snow", event_type="request_observed", request_id=f"req_{index}", anonymous_id=visitor_snow),
            dict(app="project_snow", event_type="request_complete", request_id=f"req_{index}",
                 character_id="sample_character", success=index % 5 != 0, elapsed_ms=100 + index)]
    return [base | row | dict(event_id=identifier(f"event/{index}/{j}")) for j, row in enumerate(rows)]


def lite_metrics(store):
    with store.lock:
        return [dict(row) for row in store.db.execute(
            "SELECT source,app,day AS date,pv,uv,requests,successes FROM daily ORDER BY source,day,app")]


def cursor(store):
    with store.lock:
        return int(store.db.execute("SELECT value FROM meta WHERE key='cursor'").fetchone()[0])


def main():
    env = json.loads(subprocess.check_output([*ssh, "snow@" + host,
                    "sudo docker inspect snow-lab-realtime-jobmanager-1 --format '{{json .Config.Env}}'"], text=True))
    selected = dict(value.split("=", 1) for value in env)
    assert selected["SNOW_REPLAY_LANE"] == args.lane and selected["DORIS_TABLE"] == database + ".events_realtime"
    assert selected["SNOW_SOURCE"] == "synthetic"
    assert query(f"SELECT COUNT(*) AS n FROM {database}.events_realtime")[0]["n"] == 0
    settings = Settings(mode="full", source="synthetic", db=folder / "collector/statistics.db",
                        reader_token=secrets.token_urlsafe(32), server_token=secrets.token_urlsafe(32),
                        origins=("https://snow-statistics.invalid",), allowed_characters=frozenset({"sample_character"}))
    store = Store(settings)
    server = uvicorn.Server(uvicorn.Config(create_app(settings, store), access_log=False, log_level="error"))
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    url = "http://127.0.0.1:" + str(sock.getsockname()[1])
    serve_thread = threading.Thread(target=server.run, kwargs=dict(sockets=[sock]), daemon=True)
    stop = threading.Event()
    worker = None
    errors, sync_batches = [], []
    start_wall, start_mono = time.time(), time.monotonic()
    serve_thread.start()
    try:
        until(lambda: server.started, 15)
        with flink() as client, httpx.Client(base_url=url, timeout=10, headers={
                "Origin": settings.origins[0], "Authorization": "Bearer " + settings.server_token}) as collector:
            jobs = client.get("/jobs/overview").json()["jobs"]
            assert not any(job["state"] not in ("CANCELED", "FINISHED", "FAILED") for job in jobs)
            jar = ROOT / "warehouse/flink/target/snow-realtime-0.1.0.jar"
            with jar.open("rb") as stream:
                response = client.post("/jars/upload", files={"jarfile": (jar.name, stream, "application/java-archive")}, timeout=120)
            response.raise_for_status()
            jar_id = response.json()["filename"].rsplit("/", 1)[-1]
            response = client.post("/jars/" + jar_id + "/run", json=dict(entryClass="dev.xiaob.snow.RealtimeJob", parallelism=1))
            response.raise_for_status()
            job = response.json()["jobid"]
            write_json(folder / "job.json", dict(job_id=job, jar_sha256=digest(jar.read_bytes())))
            until(lambda: client.get("/jobs/" + job).json()["state"] == "RUNNING")
            until(lambda: client.get("/jobs/" + job + "/checkpoints").json()["counts"]["completed"] >= 1)

            def sync_worker():
                try:
                    kafka_sync(url, settings.reader_token, host + ":9092", folder / "sync", lane=args.lane,
                               source="synthetic", follow=True, stop=stop, poll_seconds=1,
                               on_batch=lambda n: sync_batches.append(dict(monotonic=time.monotonic(), rows=n)))
                except Exception as error:
                    errors.append(type(error).__name__)

            worker = threading.Thread(target=sync_worker, daemon=True)
            worker.start()
            pending, samples, sent = {}, [], []
            def observe():
                if errors:
                    raise RuntimeError("Sync worker failed: " + errors[0])
                visible = query(f"SELECT app,business_key,accepted_at FROM {database}.events_realtime")
                completed = time.monotonic()
                for row in visible:
                    key = (row["app"], row["business_key"])
                    if key in pending:
                        value = pending.pop(key)
                        samples.append(dict(event_id=value["event_id"], accepted_at=str(row["accepted_at"]),
                                            from_send_seconds=completed-value["send"], from_ack_seconds=completed-value["ack"]))
                return not pending

            measurement_start = time.monotonic()
            for i in range(60):
                rows = wave(i)
                send_time = time.monotonic()
                response = collector.post("/analytics/v1/events", json=dict(events=rows))
                ack_time = time.monotonic()
                assert response.status_code == 202 and response.json() == dict(accepted=5, duplicates=0)
                for row in rows:
                    key = "request:" + row["request_id"] if row["event_type"] == "request_complete" else "event:" + row["event_id"]
                    pending[(row["app"], key)] = dict(event_id=row["event_id"], send=send_time, ack=ack_time)
                sent.extend(rows)
                observe()
                if i % 10 == 9:
                    print(json.dumps(dict(waves=i+1, accepted=len(sent), observed=len(samples))), flush=True)
                    capacity(1024)
                time.sleep(max(0, 1 - (time.monotonic() - send_time)))
            send_duration = time.monotonic() - measurement_start
            until(observe)
            freshness = summarize(samples, 300)
            write_json(folder / "latency-samples.json", samples)
            write_json(folder / "freshness.json", freshness)
            assert freshness["passed"], "P95 target failed; retain all observed samples"
            stop.set()
            worker.join(timeout=35)
            assert not worker.is_alive() and not errors
            assert json.loads((folder / "sync/cursor.json").read_bytes())["cursor"] == 300
            assert not (folder / "sync/pending.json").exists()
            until(lambda: cursor(store) == 300)
            envelopes = store.read(limit=500)["events"]
            expected = daily_metrics(envelopes)
            actual = [dict(source=r["source"], app=r["app"], date=str(r["business_date"]),
                           **{k: int(r[k]) for k in ("pv", "uv", "requests", "successes")})
                      for r in query(f"SELECT * FROM {database}.daily_realtime ORDER BY source,business_date,app")]
            assert actual == lite_metrics(store) == expected
            summary = collector.get("/analytics/public/v1/summary.json").json()
            assert summary["daily"] == [] and summary["popularity"] == [] and summary["status"] == "empty"
            checkpoints = client.get("/jobs/" + job + "/checkpoints").json()
            write_json(folder / "checkpoints.json", checkpoints)
            write_json(folder / "accepted.json", envelopes)
            write_json(folder / "sync-batches.json", sync_batches)
            subprocess.run([sys.executable, "tools/smoke_realtime.py", "--action", "archive", "--lane", args.lane, "--host", host], check=True)
            archived = ROOT / "runtime/realtime" / args.lane
            for name in ("late", "duplicates", "quarantine"):
                assert json.loads((archived / (name + ".json")).read_bytes())["rows"] == []
            kafka_archive = json.loads((archived / "events.json").read_bytes())
            assert kafka_archive["end"] - kafka_archive["begin"] == 300
            assert [row["value"] for row in kafka_archive["rows"]] == envelopes
            print(json.dumps(dict(freshness=freshness, metrics_equal=True)), flush=True)

        # The collector and its real background aggregation thread remain alive.
        subprocess.run([sys.executable, "tools/lab_remote.py", "--node", "snow-analysis", "--script", "tools/stop_realtime_node.sh"], check=True)
        subprocess.run([sys.executable, "tools/vmware_lab.py", "stop", "--node", "snow-analysis"], check=True)
        assert "Total running VMs: 0" in run(VMWARE / "vmrun.exe", "-T", "ws", "list")
        print("All warehouse VMs off; checking collector and its ordinary 60-second aggregation worker", flush=True)
        with httpx.Client(base_url=url, timeout=10, headers={"Authorization": "Bearer " + settings.server_token}) as collector:
            response = collector.post("/analytics/v1/events", json=dict(events=wave(60)))
            assert response.status_code == 202 and response.json() == dict(accepted=5, duplicates=0)
            until(lambda: cursor(store) == 305)
            final_metrics = lite_metrics(store)
            assert final_metrics == daily_metrics(store.read(limit=500)["events"])
            public = collector.get("/analytics/public/v1/summary.json").json()
            assert public["status"] == "empty" and public["daily"] == [] and public["generated_at"] != summary["generated_at"]
        wall_drift = abs((time.time()-start_wall) - (time.monotonic()-start_mono))
        assert wall_drift < .25, "Host wall clock stepped during the test"
        receipt = dict(schema_version=1, source="synthetic", lane=args.lane,
                       engines="HTTP/FastAPI + SQLite FULL WAL -> archived sync -> Kafka 3.9.1 -> Flink 1.20.3 -> Doris 3.0.6.2",
                       job_id=job, jar_sha256=digest(jar.read_bytes()), metrics=actual, freshness=freshness,
                       measurement=dict(events=300, waves=60, events_per_wave=5, send_duration_seconds=send_duration,
                                        sync_poll_seconds=1, query_poll_seconds_approx=1, checkpoint_interval_seconds=10,
                                        started_with_no_backlog=True, host_wall_vs_monotonic_drift_seconds=wall_drift),
                       reconciliation=dict(accepted=300, archived=300, kafka=300, doris=300, late=0, duplicates=0, quarantine=0,
                                           lite_doris_oracle_equal=True),
                       checkpoints=dict(counts=checkpoints["counts"], latest_completed=checkpoints["latest"]["completed"]["id"]),
                       lite_after_shutdown=dict(running_vms=0, accepted_additional=5, aggregate_cursor=305,
                                                metrics=final_metrics, public_synthetic_rows=0,
                                                public_snapshot_refreshed=True, aggregate_interval_seconds=60,
                                                warehouse_not_required=True, sync_cursor_preserved=300),
                       resources_after_shutdown=capacity(),
                       scope="Local synthetic single-host small sample. Collector and observer use host loopback, warehouse uses NAT. No production Internet latency or sustained load claim")
        write_json(folder / "receipt.json", receipt)
        print(json.dumps(receipt), flush=True)
    finally:
        stop.set()
        if worker:
            worker.join(timeout=35)
        server.should_exit = True
        serve_thread.join(timeout=15)
        if serve_thread.is_alive():
            raise RuntimeError("Collector did not exit cleanly")
        store.close()
        sock.close()


if __name__ == "__main__":
    main()
