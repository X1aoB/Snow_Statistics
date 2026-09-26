"""Root-only Docker reader; no token, business writes, shell, or caller paths.

The only client request is an integer cursor over the projected, bounded spool.
Docker log bytes are never written to disk or diagnostics. The reader polls both
colours, including stopped/draining containers, and reports incomplete coverage.
"""
import json
import os
import re
import selectors
import socket
import sqlite3
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

PROJECT = "project-snow-public"
SERVICES = {"public-api-blue", "public-api-green"}
SOCKET = Path("/run/snow-statistics-log/reader.sock")
STATE = Path("/var/lib/snow-statistics-reader/spool.db")
LOOKBACK = 600
MAX_LINES = 2000
MAX_ROWS = 4096
RETENTION = 86400
DOCKER = "/usr/bin/docker"


def timestamp(value):
    # Docker uses RFC3339Nano; only microseconds are needed by event v1.
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Timezone required")
    return parsed.astimezone(UTC)


def project_line(line, now):
    """Drop arbitrary log fields, including terminal error text, before spooling."""
    try:
        if len(line) > 65536 or b'"public_generation_complete"' not in line:
            return None
        stamp, body = line.decode("utf-8").split(" ", 1)
        occurred = timestamp(stamp)
        if not now - timedelta(seconds=RETENTION) <= occurred <= now + timedelta(minutes=5):
            return None
        record = json.loads(body[body.index('{"event"'):])
        if record.get("event") != "public_generation_complete":
            return None
        for key in ("request_id", "character_id"):
            if not isinstance(record.get(key), str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", record[key]):
                return None
        elapsed = record.get("elapsed_ms")
        if type(elapsed) is not int or not 0 <= elapsed <= 86400000:
            return None
        return dict(schema_version=1, event_id=str(uuid5(NAMESPACE_URL, "snow/request/" + record["request_id"])),
                    app="project_snow", event_type="request_complete", occurred_at=occurred.isoformat(),
                    request_id=record["request_id"], character_id=record["character_id"], elapsed_ms=elapsed,
                    success=record.get("stage") == "complete" and not record.get("terminal_error")
                    and not record.get("exception_type"))
    except (ValueError, TypeError, KeyError, UnicodeError):
        return None


class Spool:
    def __init__(self, path=STATE, max_rows=MAX_ROWS):
        if not 1 <= max_rows <= MAX_ROWS:
            raise ValueError("Invalid spool bound")
        self.max_rows = max_rows
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA journal_mode=DELETE")
        self.db.execute("PRAGMA secure_delete=ON")
        self.db.execute("PRAGMA max_page_count=4096")  # 16 MiB including reusable pages; no WAL.
        self.db.executescript("""
          CREATE TABLE IF NOT EXISTS events(seq INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT UNIQUE NOT NULL, payload TEXT NOT NULL, expires REAL NOT NULL);
          CREATE TABLE IF NOT EXISTS metrics(name TEXT PRIMARY KEY,value REAL NOT NULL);
        """)

    def add_metric(self, name, value=1):
        self.db.execute("INSERT INTO metrics VALUES(?,?) ON CONFLICT(name) DO UPDATE SET value=value+excluded.value",
                        (name, value))

    def metric(self, name):
        row = self.db.execute("SELECT value FROM metrics WHERE name=?", (name,)).fetchone()
        return row[0] if row else 0

    def ingest(self, events, now, *, incomplete=False):
        with self.db:
            previous = self.metric("last_poll")
            if previous and now - previous > LOOKBACK:
                self.add_metric("unobserved_seconds", now - previous - LOOKBACK)
            if not previous:
                self.add_metric("coverage_started_at", now - LOOKBACK)
            if incomplete:
                self.add_metric("incomplete_polls")
            self.db.execute("INSERT OR REPLACE INTO metrics VALUES('last_poll',?)", (now,))
            for event in events:
                expires = timestamp(event["occurred_at"]).timestamp() + RETENTION
                if expires <= now:
                    continue
                self.db.execute("INSERT OR IGNORE INTO events(event_id,payload,expires) VALUES(?,?,?)",
                                (event["event_id"], json.dumps(event, separators=(",", ":")), expires))
            expired = self.db.execute("SELECT COUNT(*) FROM events WHERE expires<=?", (now,)).fetchone()[0]
            self.db.execute("DELETE FROM events WHERE expires<=?", (now,))
            self.add_metric("expired_records", expired)
            count = self.db.execute("SELECT COUNT(*) FROM events").fetchone()[0]
            excess = max(0, count - self.max_rows)
            self.db.execute("DELETE FROM events WHERE seq IN (SELECT seq FROM events ORDER BY seq LIMIT ?)", (excess,))
            self.add_metric("capacity_dropped_records", excess)

    def read(self, after, now):
        if type(after) is not int or not 0 <= after < 2**63:
            raise ValueError("Invalid cursor")
        rows = self.db.execute("SELECT seq,payload FROM events WHERE seq>? AND expires>? ORDER BY seq LIMIT 50",
                               (after, now)).fetchall()
        # Sequence holes include dedup-conflict AUTOINCREMENT reservations; never
        # infer exact lost event counts from seq arithmetic. Global counters are exact.
        high = self.db.execute("SELECT seq FROM sqlite_sequence WHERE name='events'").fetchone()
        return dict(schema_version=1, records=[dict(seq=seq, event=json.loads(value)) for seq, value in rows],
                    next_cursor=rows[-1][0] if rows else max(after, high[0] if high else 0),
                    metrics=dict(self.db.execute("SELECT name,value FROM metrics")))


def docker_output(args, limit=65536, timeout=8):
    """Bound stdout/stderr even when Docker or an application emits huge lines."""
    child = subprocess.Popen([DOCKER, *args], stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, env={"PATH": "/usr/bin:/bin", "HOME": "/nonexistent"})
    output = bytearray()
    deadline = time.monotonic() + timeout
    selector = selectors.DefaultSelector()
    selector.register(child.stdout, selectors.EVENT_READ)
    incomplete = False
    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                incomplete = True
                break
            ready = selector.select(min(remaining, 0.2))
            if not ready:
                continue
            chunk = os.read(child.stdout.fileno(), min(8192, limit + 1 - len(output)))
            if not chunk:
                break
            output.extend(chunk)
            if len(output) > limit:
                incomplete = True
                break
    finally:
        selector.close()
        if child.poll() is None:
            try:
                child.wait(timeout=0.2)
            except subprocess.TimeoutExpired:
                child.terminate()
        try:
            child.wait(timeout=2)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait(timeout=2)
        child.stdout.close()
    return bytes(output[:limit]), incomplete or child.returncode != 0


def collect(now, run=docker_output):
    deadline = time.monotonic() + 40
    output, incomplete = run(["ps", "-aq", "--no-trunc", "--filter", "label=com.docker.compose.project=" + PROJECT])
    identifiers = output.decode("ascii", errors="ignore").splitlines()
    if len(identifiers) > 64:
        return [], True
    events, targets = [], 0
    for ident in identifiers:
        if time.monotonic() > deadline:
            incomplete = True
            break
        if not re.fullmatch(r"[a-f0-9]{64}", ident):
            incomplete = True
            continue
        raw, error = run(["inspect", "--format", '{{json .Config.Labels}}', ident])
        if error:
            incomplete = True
            continue
        try:
            labels = json.loads(raw)
        except (ValueError, TypeError):
            incomplete = True
            continue
        if labels.get("com.docker.compose.project") != PROJECT or labels.get("com.docker.compose.service") not in SERVICES:
            continue
        targets += 1
        if targets > 4:
            incomplete = True
            break
        since = (now - timedelta(seconds=LOOKBACK)).isoformat()
        raw, error = run(["logs", "--timestamps", "--since", since, "--tail", str(MAX_LINES), ident],
                         limit=2 * 1024**2, timeout=5)
        lines = raw.splitlines()
        incomplete |= error or len(lines) >= MAX_LINES
        for line in lines:
            event = project_line(line, now)
            if event is not None:
                events.append(event)
    return sorted(events, key=lambda e: (e["occurred_at"], e["event_id"])), incomplete or targets == 0


def serve(spool, server, *, poll=collect, stop=None, poll_interval=10):
    """Serve committed batches while one bounded Docker poll runs separately.

    Only this thread owns SQLite. The worker returns already projected events;
    there is one in-flight future and no queued polls or raw-log handoff. A slow
    poll leaves last_poll unchanged, so stale coverage is visible to clients.
    """
    stop = stop if stop is not None else threading.Event()
    server.settimeout(0.5)
    next_poll, pending = 0.0, None
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="completion-log-poll") as worker:
        while not stop.is_set():
            if pending is not None and pending.done():
                try:
                    events, incomplete = pending.result()
                    spool.ingest(events, time.time(), incomplete=incomplete)
                except Exception:
                    with spool.db:
                        spool.add_metric("reader_failures")
                pending = None
                next_poll = time.monotonic() + poll_interval
            if pending is None and time.monotonic() >= next_poll:
                pending = worker.submit(poll, datetime.now(UTC))
            try:
                connection, _ = server.accept()
            except TimeoutError:
                continue
            with connection:
                connection.settimeout(1)
                try:
                    request = bytearray()
                    while b"\n" not in request and len(request) <= 128:
                        data = connection.recv(129 - len(request))
                        if not data:
                            break
                        request.extend(data)
                    if len(request) > 128:
                        raise ValueError("Oversized request")
                    value = json.loads(request)
                    if set(value) != {"after"}:
                        raise ValueError("Unknown operation")
                    result = spool.read(value["after"], time.time())
                    connection.sendall(json.dumps(result, separators=(",", ":")).encode() + b"\n")
                except (ValueError, TypeError, OSError):
                    pass


def main():
    import grp
    import sys
    if len(sys.argv) != 1 or os.geteuid() != 0:
        raise SystemExit("Root reader accepts no arguments")
    spool = Spool()
    if SOCKET.exists():
        if not SOCKET.is_socket():
            raise SystemExit("Unexpected runtime socket path")
        SOCKET.unlink()
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(SOCKET))
    os.chown(SOCKET, 0, grp.getgrnam("snow-statistics-log").gr_gid)
    os.chmod(SOCKET, 0o660)
    server.listen(4)
    try:
        serve(spool, server)
    finally:
        spool.db.close()
        server.close()
        SOCKET.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
