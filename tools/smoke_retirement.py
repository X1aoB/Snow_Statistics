"""Real loopback process lifecycle, bounded log follower and retained export.

Real-labelled fixtures exercise the public API in an isolated, private database;
they are synthetic test inputs and never sent to a public service.
"""
import argparse
import json
import os
import secrets
import socket
import sqlite3
import subprocess
import sys
import threading
import time
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import httpx
import uvicorn

from snow_statistics.api import create_app
from snow_statistics.config import Settings
from snow_statistics.io import digest, write_json

parser = argparse.ArgumentParser()
parser.add_argument("--output", type=Path, required=True)
parser.add_argument("--serve", choices=("full", "lite", "off"))
parser.add_argument("--port", type=int)
args = parser.parse_args()
folder = args.output.resolve()
if args.serve:
    settings = Settings(mode=args.serve, db=folder / "state/statistics.db", aggregate_interval=.2,
                        server_token=os.environ["SNOW_SERVER_TOKEN"], reader_token=os.environ["SNOW_READER_TOKEN"],
                        allowed_characters=frozenset({"fixture_character"}), origins=("http://127.0.0.1",))
    server = uvicorn.Server(uvicorn.Config(create_app(settings), host="127.0.0.1", port=args.port,
                                           access_log=False, log_level="warning"))
    thread = threading.Thread(target=server.run)
    thread.start()
    try:
        sys.stdin.readline()  # Private supervisor pipe, never a public shutdown route.
    finally:
        server.should_exit = True
        thread.join(timeout=10)
        if thread.is_alive():
            raise RuntimeError("Collector did not stop cleanly")
    raise SystemExit(0)

folder.mkdir(parents=True, exist_ok=False)
env = os.environ | {"SNOW_SERVER_TOKEN": secrets.token_urlsafe(32), "SNOW_READER_TOKEN": secrets.token_urlsafe(32)}
env.pop("SNOW_LINEAGE_ENABLED", None)
process = log_file = None
history = []
headers = {"Origin": "http://127.0.0.1"}


def until(predicate):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        try:
            result = predicate()
            if result:
                return result
        except (httpx.HTTPError, ValueError, KeyError):
            pass
        time.sleep(.1)
    raise RuntimeError("Retirement acceptance deadline")


def stop():
    global process, log_file
    if process:
        try:
            process.communicate("stop\n", timeout=15)
            assert process.returncode == 0
        except BaseException:
            process.kill()
            process.wait(timeout=5)
            raise
        finally:
            process = None
    if log_file:
        log_file.close()
        log_file = None


def start(mode):
    global process, log_file
    stop()
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    log_file = (folder / (mode + ".log")).open("wb")
    process = subprocess.Popen([sys.executable, __file__, "--output", str(folder), "--serve", mode, "--port", str(port)],
                               env=env, stdin=subprocess.PIPE, text=True, stdout=log_file, stderr=subprocess.STDOUT,
                               creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
    url = f"http://127.0.0.1:{port}"
    until(lambda: httpx.get(url + "/healthz", timeout=1).json()["mode"] == mode)
    return url


def summary(url):
    return httpx.get(url + "/analytics/public/v1/summary.json", timeout=2).json()


def metrics(url, pv):
    data = summary(url)
    rows = data["daily"]
    return data if sum(r["pv"] for r in rows) == pv and sum(r["requests"] for r in rows) == 1 else None


try:
    url = start("full")
    now = datetime.now(UTC).isoformat()
    page = dict(event_id=str(uuid4()), occurred_at=now, app="mywebsite", event_type="page_view", path="/",
                anonymous_id=str(uuid4()))
    assert httpx.post(url + "/analytics/v1/events", json={"events": [page]}, headers=headers).json() == {"accepted": 1, "duplicates": 0}
    record = dict(event="public_generation_complete", request_id="retirement-request", character_id="fixture_character",
                  elapsed_ms=42, stage="complete", chat_body="private-fixture-sentinel", ip="private-ip-sentinel")
    line = now + " " + json.dumps(record, separators=(",", ":")) + "\n"
    # Complete malformed/oversized records precede a valid record, plus replay.
    stream = "malformed\n" + ("x" * 200000) + "\n" + line * 2
    command = [sys.executable, str(Path(__file__).with_name("forward_logs.py")), "--url", url]
    disabled = subprocess.run(command, input=line, text=True, capture_output=True, env=env, timeout=10)
    assert disabled.returncode != 0
    follower = subprocess.run([*command, "--enabled"], input=stream, text=True, capture_output=True, env=env, timeout=15)
    assert follower.returncode == 0 and not follower.stdout and not follower.stderr
    before = until(lambda: metrics(url, 1))
    assert sum(r["successes"] for r in before["daily"]) == 1
    history.append(dict(mode="full", daily=before["daily"]))
    url = start("lite")
    assert summary(url)["daily"] == before["daily"]
    assert httpx.post(url + "/analytics/v1/events", json={"events": [page]}, headers=headers).json() == {"accepted": 0, "duplicates": 1}
    page["event_id"] = str(uuid4())
    assert httpx.post(url + "/analytics/v1/events", json={"events": [page]}, headers=headers).json()["accepted"] == 1
    latest = until(lambda: metrics(url, 2))
    history.append(dict(mode="lite", daily=latest["daily"]))
    raw = httpx.get(url + "/analytics/private/v1/events", headers={"Authorization": "Bearer " + env["SNOW_READER_TOKEN"]}).json()
    assert len(raw["events"]) == 3
    assert "private-fixture-sentinel" not in json.dumps(raw) and "private-ip-sentinel" not in json.dumps(raw)
    (folder / "exports").mkdir()
    write_json(folder / "exports/summary-before-off.json", latest)
    with closing(sqlite3.connect(f"file:{folder / 'state/statistics.db'}?mode=ro", uri=True)) as source:
        with closing(sqlite3.connect(folder / "exports/statistics.sqlite")) as target:
            source.backup(target)
            target.execute("PRAGMA journal_mode=DELETE")
    url = start("off")
    response = httpx.post(url + "/analytics/v1/events", json={"events": [page]}, headers=headers)
    assert response.status_code == 503
    archived = summary(url)
    assert archived["status"] == "archived" and archived["daily"] == latest["daily"]
    assert set(archived) == {"schema_version", "generated_at", "date_from", "date_to", "status", "completeness", "daily", "popularity"}
    write_json(folder / "exports/summary-archived.json", archived)
    history.append(dict(mode="off", status=archived["status"], daily=archived["daily"]))
    stop()
    with closing(sqlite3.connect(folder / "state/statistics.db")) as source, closing(sqlite3.connect(folder / "exports/statistics.sqlite")) as copy:
        assert list(source.execute("SELECT seq,payload FROM events ORDER BY seq")) == list(copy.execute("SELECT seq,payload FROM events ORDER BY seq"))
        assert source.execute("PRAGMA integrity_check").fetchone() == ("ok",)
    # An unavailable destination also lets the external reader terminate; it never controls a business producer.
    unavailable = subprocess.run([*command, "--enabled"], input=line, text=True, capture_output=True, env=env, timeout=12)
    assert unavailable.returncode == 0 and not unavailable.stdout and not unavailable.stderr
    resources = dict(owner="Snow_Statistics", scope="isolated loopback acceptance only", root=str(folder),
                     processes_stopped=True, database_preserved=True,
                     exported_files=[dict(path=p.name, sha256=digest(p.read_bytes())) for p in sorted((folder / "exports").iterdir())],
                     deletion="Review only; no historical database, VM, product file or volume deleted")
    write_json(folder / "removal-review.json", resources)
    result = dict(scope="Synthetic real-labelled fixtures, private loopback deployment; all warehouse VMs off",
                  modes=history, retained_events=3, database_integrity="ok", exact_export_equal=True,
                  duplicate_log_and_event_dedup=True, oversized_log_drained=True, private_log_fields_excluded=True,
                  collector_unavailable_follower_exits=True, disabled_follower_exits_without_sending=True,
                  stopped_without_deleting=True, export_hashes=resources["exported_files"])
    write_json(folder / "accepted.json", result)
    print(json.dumps(result))
finally:
    stop()
