"""Unprivileged reader of safe projected events. No Docker socket or business state."""
import json
import os
import re
import socket
import time
from datetime import datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener
from uuid import UUID


def write_json(path, value):
    temporary = path.with_suffix(".tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(value, stream, separators=(",", ":"))
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def validate_event(event):
    if set(event) != {"schema_version", "event_id", "app", "event_type", "occurred_at",
                      "request_id", "character_id", "elapsed_ms", "success"}:
        raise ValueError("Unexpected event fields")
    if event["schema_version"] != 1 or event["app"] != "project_snow" or event["event_type"] != "request_complete":
        raise ValueError("Unexpected event")
    UUID(event["event_id"])
    if datetime.fromisoformat(event["occurred_at"].replace("Z", "+00:00")).tzinfo is None:
        raise ValueError("Missing timezone")
    if type(event["success"]) is not bool or type(event["elapsed_ms"]) is not int or not 0 <= event["elapsed_ms"] <= 86400000:
        raise ValueError("Invalid metrics")
    if any(not isinstance(event[k], str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", event[k]) for k in ("request_id", "character_id")):
        raise ValueError("Invalid identifiers")
    return event

SOCKET = "/run/snow-statistics-log/reader.sock"
STATE = Path("/var/lib/snow-statistics-forwarder/cursor.json")
URL = "http://127.0.0.1:8100/analytics/v1/events"


def read_batch(after):
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as stream:
        stream.settimeout(3)
        stream.connect(SOCKET)
        stream.sendall(json.dumps({"after": after}).encode() + b"\n")
        body = bytearray()
        while b"\n" not in body and len(body) <= 65536:
            chunk = stream.recv(min(8192, 65537 - len(body)))
            if not chunk:
                break
            body.extend(chunk)
    if len(body) > 65536:
        raise ValueError("Oversized projected batch")
    return json.loads(body)


def forward_once(path, fetch, send):
    """Only a 202 advances acknowledged state. Crashes safely replay stable IDs."""
    state = json.loads(path.read_bytes()) if path.exists() else {"cursor": 0, "acknowledged": 0, "rejected": 0}
    batch = fetch(state["cursor"])
    if batch.get("schema_version") != 1 or not isinstance(batch.get("records"), list) or len(batch["records"]) > 50:
        raise ValueError("Invalid projected batch")
    cursor = state["cursor"]
    events = []
    for row in batch["records"]:
        if type(row["seq"]) is not int or row["seq"] <= cursor:
            raise ValueError("Non-monotonic projected batch")
        cursor = row["seq"]
        events.append(validate_event(row["event"]))
    if type(batch["next_cursor"]) is not int or batch["next_cursor"] < cursor:
        raise ValueError("Invalid reader progress")
    state["reader_metrics"] = batch["metrics"]
    state["observed_at"] = time.time()
    if events:
        status = send(events)
        state["last_status"] = status
        if status == 202:
            state["acknowledged"] += len(events)
        elif status in (409, 413, 415, 422):
            # Keep exact rejected counts; do not block newer known-good records
            # forever. There is no raw-event/error-body diagnostic export.
            state["rejected"] += len(events)
        else:
            write_json(path, state)
            return False
    state["cursor"] = batch["next_cursor"]
    write_json(path, state)
    return True


def main():
    if os.getenv("SNOW_LOG_FORWARD_ENABLED", "false").lower() != "true":
        return
    token_path = Path(os.environ["CREDENTIALS_DIRECTORY"]) / "server-token"
    token = token_path.read_text().strip()
    if not token or len(token) > 4096:
        raise SystemExit("Missing server credential")
    class NoRedirect(HTTPRedirectHandler):
        def redirect_request(self, *args, **kwargs):
            return None
    opener = build_opener(ProxyHandler({}), NoRedirect())
    def send(events):
        request = Request(URL, data=json.dumps({"events": events}).encode(), method="POST",
                          headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"})
        try:
            with opener.open(request, timeout=2) as response:
                return response.status
        except HTTPError as error:
            status = error.code
            error.close()
            return status
    while True:
        try:
            forward_once(STATE, read_batch, send)
        except (OSError, ValueError, KeyError, TypeError, URLError):
            # Safe local status only: no response bodies or original logs.
            state = json.loads(STATE.read_bytes()) if STATE.exists() else {"cursor": 0, "acknowledged": 0, "rejected": 0}
            state["failures"] = state.get("failures", 0) + 1
            state["observed_at"] = time.time()
            write_json(STATE, state)
        time.sleep(10)


if __name__ == "__main__":
    main()
