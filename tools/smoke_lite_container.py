"""Linux-only disposable 2 GiB filesystem/container acceptance; no production input.

Run with sudo after building snow-statistics-lite:0.1.0. All fixtures (including
real-labelled metric fixtures) stay in a new private directory and loopback port.
Only the named test container is removed; the unmounted database image is retained.
"""
import errno
import json
import os
import secrets
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4


def command(*args, **kwargs):
    return subprocess.run(args, check=True, capture_output=True, text=True, **kwargs).stdout.strip()


def main():
    if os.name != "posix" or os.geteuid() != 0:
        raise SystemExit("Run inside the isolated Linux lab with sudo.")
    root = Path(__file__).resolve().parents[1]
    directory = Path(tempfile.mkdtemp(prefix="lite-container-", dir=root / "runtime"))
    state = directory / "state"
    state.mkdir()
    disk = directory / "state.img"
    name = "snow-lite-acceptance-" + directory.name.removeprefix("lite-container-")
    env = os.environ | {"SNOW_SERVER_TOKEN": secrets.token_urlsafe(32), "SNOW_READER_TOKEN": secrets.token_urlsafe(32)}
    receipt = {"scope": "isolated_loopback_fixture", "quota_bytes": 2 * 1024**3}
    mounted = running = False
    base = ""

    def request(path, payload=None, *, trusted=False, reader=False):
        headers = {"Content-Type": "application/json", "Origin": "http://127.0.0.1"}
        if trusted or reader:
            headers["Authorization"] = "Bearer " + env["SNOW_READER_TOKEN" if reader else "SNOW_SERVER_TOKEN"]
        req = urllib.request.Request(base + path, data=json.dumps(payload).encode() if payload else None, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=3) as response:
                return response.status, json.load(response)
        except urllib.error.HTTPError as error:
            return error.code, json.load(error)

    def wait_for(predicate, seconds=15):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            try:
                value = predicate()
                if value:
                    return value
            except (OSError, ValueError):
                pass
            time.sleep(0.1)
        raise AssertionError("acceptance condition timed out")

    def start(mode):
        nonlocal base, running
        if running:
            command("docker", "rm", "-f", name)
            running = False
        script = ("from dataclasses import replace; import uvicorn; "
                  "from snow_statistics.api import create_app; from snow_statistics.config import Settings; "
                  "uvicorn.run(create_app(replace(Settings.from_env(), aggregate_interval=0.2)), "
                  "host='0.0.0.0',port=8100,access_log=False)")
        command("docker", "run", "-d", "--name", name, "--read-only", "--memory", "512m", "--cpus", "0.5",
                "--pids-limit", "128", "--cap-drop", "ALL", "--security-opt", "no-new-privileges:true",
                "--tmpfs", "/tmp:size=32m", "-p", "127.0.0.1::8100", "-v", f"{state}:/state",
                "-e", "SNOW_SERVER_TOKEN", "-e", "SNOW_READER_TOKEN", "-e", f"SNOW_MODE={mode}",
                "-e", "SNOW_ORIGINS=http://127.0.0.1", "-e", "SNOW_ALLOWED_CHARACTERS=fixture_character",
                "snow-statistics-lite:0.1.0", "/app/.venv/bin/python", "-c", script, env=env)
        running = True
        base = "http://" + command("docker", "port", name, "8100/tcp")
        wait_for(lambda: request("/healthz")[0] == 200)

    try:
        command("fallocate", "-l", "2G", str(disk))
        command("mkfs.ext4", "-q", "-F", "-m", "0", str(disk))
        command("mount", "-o", "loop,nodev,nosuid,noexec", str(disk), str(state))
        mounted = True
        os.chown(state, 10001, 10001)
        start("lite")
        now = datetime.now(UTC).isoformat()
        page = {"event_id": str(uuid4()), "app": "mywebsite", "event_type": "page_view", "occurred_at": now,
                "path": "/", "anonymous_id": str(uuid4())}
        complete = {"event_id": str(uuid4()), "app": "project_snow", "event_type": "request_complete", "occurred_at": now,
                    "request_id": "fixture-request", "character_id": "fixture_character", "success": True, "elapsed_ms": 120}
        events = {"events": [page, complete]}
        assert request("/analytics/v1/events", events)[0] == 401
        assert request("/analytics/v1/events", events, trusted=True) == (202, {"accepted": 2, "duplicates": 0})
        summary_path = "/analytics/public/v1/summary.json"
        daily = wait_for(lambda: request(summary_path)[1]["daily"])
        assert sum(r["pv"] for r in daily) == sum(r["uv"] for r in daily) == 1
        assert sum(r["requests"] for r in daily) == sum(r["successes"] for r in daily) == 1
        receipt["lite_without_warehouse"] = True
        command("docker", "kill", name)
        command("docker", "start", name)
        base = "http://" + command("docker", "port", name, "8100/tcp")
        wait_for(lambda: request("/healthz")[0] == 200)
        assert request("/analytics/v1/events", events, trusted=True) == (202, {"accepted": 0, "duplicates": 2})
        assert request(summary_path)[1]["daily"] == daily
        receipt["crash_restart_deduplication"] = True
        for mode in ("full", "lite"):
            start(mode)
            assert request(summary_path)[1]["daily"] == daily
        receipt["full_to_lite_preserves_metrics"] = True
        # Exhaust the dedicated filesystem, never the guest or host root disk.
        stats = os.statvfs(state)
        with (state / "capacity-fixture").open("wb", buffering=0) as filler:
            os.posix_fallocate(filler.fileno(), 0, max(0, stats.f_bavail * stats.f_frsize - 8 * 1024**2))
            filler.seek(0, os.SEEK_END)
            try:
                while True:
                    filler.write(bytes(4096))
            except OSError as error:
                if error.errno != errno.ENOSPC:
                    raise
        new_page = page | {"event_id": str(uuid4())}
        code, _ = request("/analytics/v1/events", {"events": [new_page]})
        assert code == 503, code
        retained = request("/analytics/private/v1/events", reader=True)[1]
        assert len(retained["events"]) == 2
        receipt["full_filesystem_rejects_without_losing_acknowledged_events"] = True
        (state / "capacity-fixture").unlink()
        start("off")
        assert request("/analytics/v1/events", events, trusted=True)[0] == 503
        archived = request(summary_path)[1]
        assert archived["status"] == "archived" and archived["daily"] == daily
        assert set(archived) == {"schema_version", "generated_at", "date_from", "date_to", "status", "completeness", "daily", "popularity"}
        receipt["off_archives_without_deleting"] = True
        receipt["public_field_allowlist"] = True
        receipt["image_id"] = command("docker", "image", "inspect", "snow-statistics-lite:0.1.0", "--format", "{{.Id}}")
        (root / "runtime/lite-container-receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
        print(json.dumps(receipt, indent=2))
    except Exception:
        if running:
            logs = subprocess.run(["docker", "logs", name], capture_output=True, text=True)
            (directory / "failure.log").write_text(logs.stdout + logs.stderr)
        print(f"Acceptance failed; private fixture diagnostics: {directory}")
        raise
    finally:
        if running:
            command("docker", "rm", "-f", name)
        if mounted:
            command("umount", str(state))


if __name__ == "__main__":
    main()
