"""Linux-only bounded filesystem/container acceptance; no production input.

Run with sudo after building snow-statistics-lite:0.1.0. All fixtures (including
real-labelled metric fixtures) stay in a new private directory and loopback port.
Only the named test container is removed; the unmounted database image is retained.
"""
import argparse
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
    parser = argparse.ArgumentParser()
    parser.add_argument("--quota-mib", type=int, choices=(512, 1024, 2048), default=512)
    parser.add_argument("--memory-mib", type=int, choices=(256, 512), default=256)
    parser.add_argument("--cpus", type=float, choices=(0.25, 0.5), default=0.25)
    parser.add_argument("--bulk-events", type=int, choices=(0, 10000), default=10000)
    parser.add_argument("--image", default="snow-statistics-lite:0.1.0")
    args = parser.parse_args()
    if os.name != "posix" or os.geteuid() != 0:
        raise SystemExit("Run inside the isolated Linux lab with sudo.")
    root = Path(__file__).resolve().parents[1]
    directory = Path(tempfile.mkdtemp(prefix="lite-container-", dir=root / "runtime"))
    state = directory / "state"
    state.mkdir()
    disk = directory / "state.img"
    name = "snow-lite-acceptance-" + directory.name.removeprefix("lite-container-")
    env = os.environ | {"SNOW_SERVER_TOKEN": secrets.token_urlsafe(32), "SNOW_READER_TOKEN": secrets.token_urlsafe(32)}
    receipt = {"scope": "isolated_loopback_fixture", "quota_bytes": args.quota_mib * 1024**2,
               "memory_limit_mib": args.memory_mib, "cpus": args.cpus, "aggregate_interval_seconds": 0.2}
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
        command("docker", "run", "-d", "--name", name, "--read-only", "--memory", f"{args.memory_mib}m", "--cpus", str(args.cpus),
                "--pids-limit", "128", "--cap-drop", "ALL", "--security-opt", "no-new-privileges:true",
                "--tmpfs", "/tmp:size=32m", "-p", "127.0.0.1::8100", "-v", f"{state}:/state",
                "-e", "SNOW_SERVER_TOKEN", "-e", "SNOW_READER_TOKEN", "-e", f"SNOW_MODE={mode}",
                "-e", f"SNOW_BUDGET_BYTES={args.quota_mib * 1024**2}",
                "-e", "SNOW_ORIGINS=http://127.0.0.1", "-e", "SNOW_ALLOWED_CHARACTERS=fixture_character",
                args.image, "/app/.venv/bin/python", "-c", script, env=env)
        running = True
        base = "http://" + command("docker", "port", name, "8100/tcp")
        wait_for(lambda: request("/healthz")[0] == 200)

    try:
        command("fallocate", "-l", f"{args.quota_mib}M", str(disk))
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
        if args.bulk_events:
            start("lite")
            visitors = [str(uuid4()) for _ in range(500)]
            started = time.monotonic()
            last_batch = None
            for offset in range(0, args.bulk_events, 50):
                # Stay below the unchanged public admission limit (120 requests/min).
                # This is a bounded capacity fixture, not a maximum-throughput test.
                if offset:
                    time.sleep(0.55)
                batch = []
                for i in range(offset, offset + 50):
                    if i % 2 == 0:
                        batch.append(page | {"event_id": str(uuid4()), "anonymous_id": visitors[(i // 2) % 500]})
                    else:
                        batch.append(complete | {"event_id": str(uuid4()), "request_id": f"bulk-{i}", "success": (i // 2) % 5 != 0})
                last_batch = {"events": batch}
                result = request("/analytics/v1/events", last_batch, trusted=True)
                assert result == (202, {"accepted": 50, "duplicates": 0}), (offset, result)
            target_pv = 1 + args.bulk_events // 2
            def bulk_daily():
                rows = request(summary_path)[1]["daily"]
                return rows if sum(row["pv"] for row in rows) == target_pv and sum(row["requests"] for row in rows) == target_pv else None
            bulk_rows = wait_for(bulk_daily)
            assert sum(row["uv"] for row in bulk_rows) == 501
            assert sum(row["successes"] for row in bulk_rows) == 1 + args.bulk_events * 4 // 10
            metrics = command("docker", "exec", name, "sh", "-c", "cat /sys/fs/cgroup/memory.peak; cat /sys/fs/cgroup/memory.events").splitlines()
            memory_events = {key: int(value) for key, value in (line.split() for line in metrics[1:])}
            assert memory_events["oom"] == memory_events["oom_kill"] == 0
            receipt["bulk"] = dict(events=args.bulk_events, total_accepted=args.bulk_events+2,
                                    elapsed_seconds=round(time.monotonic()-started, 3), daily=bulk_rows, batch_pause_seconds=0.55,
                                    cgroup_memory_peak_bytes=int(metrics[0]), memory_events=memory_events,
                                    note="Sequential HTTP batches on loopback with 0.2-second test aggregation; not a sustained production throughput claim")
            command("docker", "kill", name)
            command("docker", "start", name)
            base = "http://" + command("docker", "port", name, "8100/tcp")
            wait_for(lambda: request("/healthz")[0] == 200)
            assert request("/analytics/v1/events", last_batch, trusted=True) == (202, {"accepted": 0, "duplicates": 50})
            assert request(summary_path)[1]["daily"] == bulk_rows
            start("off")
            assert request(summary_path)[1]["daily"] == bulk_rows
            receipt["bulk"]["restart_and_off_preserve_metrics"] = True
            receipt["bulk"]["state_file_bytes_after_restart"] = sum(p.stat().st_size for p in state.iterdir() if p.is_file())
        receipt["image_id"] = command("docker", "image", "inspect", args.image, "--format", "{{.Id}}")
        (directory / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
        (root / f"runtime/lite-container-{args.quota_mib}-{args.memory_mib}-receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
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
