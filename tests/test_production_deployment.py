import base64
import copy
import importlib.util
import json
import socket
import threading
import time
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from snow_statistics.contracts import Event
from snow_statistics.log_adapter import from_log

ROOT = Path(__file__).resolve().parents[1]


def module(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "tools" / (name + ".py"))
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


reader = module("production_log_reader")
forwarder = module("production_log_forwarder")
installer = module("production_install")
NOW = datetime(2026, 9, 13, 3, tzinfo=UTC)


def fixture(request="request_a", at=NOW):
    record = dict(event="public_generation_complete", request_id=request, character_id="sample_character",
                  elapsed_ms=20, stage="complete", terminal_error="", exception_type="",
                  chat="private chat", ip="private address", api_key="private key")
    line = (at.isoformat() + " INFO: " + json.dumps(record, separators=(",", ":"))).encode()
    return record, line


@contextmanager
def running_reader(tmp_path, poll, seed):
    # Portable loopback transport exercises the same framing/serve loop; the
    # production entry point still creates only its permissioned AF_UNIX socket.
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(4)
    address = listener.getsockname()
    ready, stop, errors = threading.Event(), threading.Event(), []
    def run():
        spool = reader.Spool(tmp_path / "concurrent-reader.db")
        try:
            seed(spool)
            ready.set()
            reader.serve(spool, listener, poll=poll, stop=stop, poll_interval=60)
        except BaseException as error:
            errors.append(error)
            ready.set()
        finally:
            spool.db.close()
            listener.close()
    thread = threading.Thread(target=run)
    thread.start()
    try:
        assert ready.wait(3) and not errors
        yield address
    finally:
        stop.set()
        thread.join(timeout=5)
        assert not thread.is_alive(), "Reader and its one polling worker must terminate"
        assert not errors


def socket_batch(address, after=0):
    with socket.create_connection(address, timeout=2) as stream:
        stream.sendall(json.dumps({"after": after}).encode() + b"\n")
        result = bytearray()
        while b"\n" not in result:
            chunk = stream.recv(65536)
            assert chunk and len(result) < 65536
            result.extend(chunk)
    return json.loads(result)


def test_slow_poll_does_not_block_committed_socket_batches_or_overlap(tmp_path):
    entered, release = threading.Event(), threading.Event()
    now = datetime.now(UTC)
    old = reader.project_line(fixture("already_committed", at=now)[1], now)
    new = reader.project_line(fixture("next_poll", at=now)[1], now)
    polls = []
    def poll(at):
        polls.append(at)
        entered.set()
        assert release.wait(5), "Synthetic poll must be released during cleanup"
        return [new], False
    def seed(spool):
        spool.ingest([old], now.timestamp())
    with running_reader(tmp_path, poll, seed) as address:
        try:
            assert entered.wait(2)
            # The poll remains blocked throughout all three real socket reads.
            # A serial poll/accept loop would time out here before any response.
            batches = [socket_batch(address) for _ in range(3)]
            assert len(polls) == 1 and not release.is_set()
            assert all([row["event"] for row in batch["records"]] == [old] for batch in batches)
            assert all(batch["metrics"]["last_poll"] == now.timestamp() for batch in batches)
            release.set()
            deadline = time.monotonic() + 3
            while True:
                batch = socket_batch(address)
                if len(batch["records"]) == 2 or time.monotonic() >= deadline:
                    break
                time.sleep(0.01)
            assert [row["event"] for row in batch["records"]] == [old, new]
            assert len(polls) == 1
        finally:
            release.set()


def test_failed_poll_keeps_batches_and_historical_coverage_metrics(tmp_path):
    entered, release = threading.Event(), threading.Event()
    now = datetime.now(UTC)
    old = reader.project_line(fixture("retained_after_failure", at=now)[1], now)
    def poll(at):
        entered.set()
        assert release.wait(5)
        raise OSError("synthetic slow Docker failure")
    def seed(spool):
        spool.ingest([old], now.timestamp(), incomplete=True)
        with spool.db:
            spool.add_metric("reader_failures", 4)
    with running_reader(tmp_path, poll, seed) as address:
        try:
            assert entered.wait(2)
            before = socket_batch(address)
            assert before["metrics"]["reader_failures"] == 4
            release.set()
            deadline = time.monotonic() + 3
            while True:
                after = socket_batch(address)
                if after["metrics"]["reader_failures"] == 5 or time.monotonic() >= deadline:
                    break
                time.sleep(0.01)
            assert after["metrics"]["reader_failures"] == 5
            assert after["metrics"]["incomplete_polls"] == 1
            assert after["metrics"]["last_poll"] == before["metrics"]["last_poll"]
            assert after["records"] == before["records"]
        finally:
            release.set()


def test_projection_matches_existing_v1_adapter_and_drops_all_arbitrary_fields():
    record, line = fixture()
    event = reader.project_line(line, NOW)
    assert Event.model_validate(event) == Event.model_validate(from_log(record, NOW.isoformat()))
    assert "private" not in json.dumps(event)
    assert forwarder.validate_event(event) == event
    assert reader.project_line(fixture(at=NOW - timedelta(days=2))[1], NOW) is None
    assert reader.project_line(b"x" * 65537, NOW) is None
    assert reader.project_line(line.replace(b'"elapsed_ms":20', b'"elapsed_ms":true'), NOW) is None
    with pytest.raises(ValueError):
        forwarder.validate_event(event | {"chat": "private"})


def test_root_spool_replays_deduplicates_expires_and_bounds_capacity(tmp_path):
    path = tmp_path / "spool.db"
    spool = reader.Spool(path, max_rows=2)
    a = reader.project_line(fixture("a")[1], NOW)
    b = reader.project_line(fixture("b")[1], NOW)
    c = reader.project_line(fixture("c")[1], NOW)
    spool.ingest([a, b, a], NOW.timestamp())
    assert len(spool.read(0, NOW.timestamp())["records"]) == 2
    spool.db.close()
    spool = reader.Spool(path, max_rows=2)
    spool.ingest([a, b, c], NOW.timestamp() + 1, incomplete=True)
    batch = spool.read(0, NOW.timestamp())
    assert len(batch["records"]) == 2 and batch["metrics"]["capacity_dropped_records"] == 1
    assert batch["metrics"]["incomplete_polls"] == 1
    spool.ingest([], NOW.timestamp() + 86401)
    assert spool.read(0, NOW.timestamp() + 86401)["records"] == []
    assert spool.metric("unobserved_seconds") > 0
    assert spool.db.execute("PRAGMA secure_delete").fetchone()[0] == 1
    spool.db.close()
    assert path.stat().st_size <= 16 * 1024**2 and not path.with_name("spool.db-wal").exists()


def test_forwarder_never_advances_on_timeout_or_auth_failure_and_replays(tmp_path):
    spool = reader.Spool(tmp_path / "reader.db")
    spool.ingest([reader.project_line(fixture()[1], NOW)], NOW.timestamp())
    def fetch(after):
        return spool.read(after, NOW.timestamp())
    state = tmp_path / "cursor.json"
    assert not forwarder.forward_once(state, fetch, lambda events: 503)
    assert json.loads(state.read_bytes())["cursor"] == 0
    assert not forwarder.forward_once(state, fetch, lambda events: 401)
    captured = []
    def accepted_then_crash(events):
        captured.extend(events)
        raise OSError("simulated lost acknowledgement")
    with pytest.raises(OSError):
        forwarder.forward_once(state, fetch, accepted_then_crash)
    assert forwarder.forward_once(state, fetch, lambda events: captured.extend(events) or 202)
    assert captured[0] == captured[1]
    assert json.loads(state.read_bytes())["acknowledged"] == 1
    assert forwarder.forward_once(state, fetch, lambda _: pytest.fail("No duplicate HTTP request expected"))
    spool.db.close()


def test_successful_forwarding_retains_historical_failures(tmp_path):
    event = reader.project_line(fixture()[1], NOW)
    path = tmp_path / "cursor.json"
    path.write_text(json.dumps(dict(cursor=0, acknowledged=0, rejected=0, failures=33)), encoding="utf-8")
    batch = dict(schema_version=1, records=[dict(seq=1, event=event)], next_cursor=1,
                 metrics={"incomplete_polls": 2, "reader_failures": 4})
    assert forwarder.forward_once(path, lambda _: batch, lambda _: 202)
    result = json.loads(path.read_bytes())
    assert result["cursor"] == 1 and result["acknowledged"] == 1
    assert result["failures"] == 33 and result["reader_metrics"] == batch["metrics"]


def test_reader_follows_only_exact_blue_green_labels_and_reports_bounded_input():
    ids = ["a" * 64, "b" * 64, "c" * 64]
    calls = []
    def run(args, **kwargs):
        calls.append(args)
        if args[0] == "ps":
            return "\n".join(ids).encode(), False
        if args[0] == "inspect":
            service = {ids[0]: "public-api-blue", ids[1]: "public-api-green", ids[2]: "postgres"}[args[-1]]
            return json.dumps({"com.docker.compose.project": reader.PROJECT,
                               "com.docker.compose.service": service}).encode(), False
        assert args[-1] != ids[2]
        return fixture(args[-1][:1])[1] + b"\n", args[-1] == ids[1]
    events, incomplete = reader.collect(NOW, run)
    assert len(events) == 2 and incomplete
    assert len([call for call in calls if call[0] == "logs"]) == 2
    assert all("--since" in call and "--tail" in call for call in calls if call[0] == "logs")


def test_candidate_plan_is_exact_and_never_enables_or_formats():
    plan = installer.plan(ROOT)
    assert len(plan["plan_sha256"]) == 64
    assert len({row["destination"] for row in plan["files"]}) == len(plan["files"])
    assert any(r"var-lib-snow\x2dstatistics-state.mount" in row["destination"] for row in plan["files"])
    text = installer.tunnel_config("12345678-1234-4234-8234-123456789abc").decode()
    assert "stats.xiaob.dev" in text and "http://gateway:8080" in text and "http_status:404" in text
    assert "8100" not in text and "snow.xiaob.dev\n" not in text
    gateway = (ROOT / "deploy/production/Caddyfile").read_text()
    assert "path /analytics/v1/events" in gateway and "/analytics/v2/events" not in gateway
    assert "path /analytics/public/v1/summary.json /analytics/public/v2/summary.json" in gateway
    assert "header_up -Authorization" in gateway and "respond 404" in gateway
    assert "/analytics/private/" not in gateway


def test_remote_tunnel_token_is_bound_and_never_treated_as_command():
    tunnel = "12345678-1234-4234-8234-123456789abc"
    data = dict(a="a" * 32, t=tunnel, s=base64.b64encode(b"synthetic secret bytes only" * 2).decode())
    token = base64.b64encode(json.dumps(data).encode())
    assert installer.validate_tunnel_token(token + b"\n", tunnel, "a" * 32) == token
    for body, ident, account in ((token, tunnel, "b" * 32), (token, "22345678-1234-4234-8234-123456789abc", "a" * 32),
                                  (b"docker run --token " + token, tunnel, "a" * 32),
                                  (base64.b64encode(json.dumps(data | {"s": ""}).encode()), tunnel, "a" * 32)):
        with pytest.raises(ValueError):
            installer.validate_tunnel_token(body, ident, account)


def test_remote_tunnel_spec_preserves_gateway_and_limits_without_secret_in_argv():
    original = dict(name="snow-statistics-edge", services=dict(
        gateway={"image": "gateway@sha256:" + "b" * 64, "networks": {"tunnel": {}, "collector": {}}},
        tunnel=dict(image="cloudflared@sha256:" + "c" * 64, user="10002:10002", networks={"tunnel": {}},
                    mem_limit="134217728", cpus=0.1, read_only=True, pids_limit=64,
                    cap_drop=["ALL"], security_opt=["no-new-privileges:true"],
                    command=["tunnel", "run"], volumes=[{"source": "old-auth"}])))
    unchanged = copy.deepcopy(original)
    configured = installer.remote_tunnel_spec(original)
    assert original == unchanged and configured["services"]["gateway"] == original["services"]["gateway"]
    actual = configured["services"]["tunnel"]
    assert actual["command"][-2:] == ["--token-file", "/run/secrets/tunnel.token"]
    assert "--token" not in actual["command"] and len(actual["volumes"]) == 1
    assert actual["volumes"][0]["read_only"] and not actual["volumes"][0]["bind"]["create_host_path"]
    assert {k: v for k, v in actual.items() if k not in {"command", "volumes"}} == {
        k: v for k, v in original["services"]["tunnel"].items() if k not in {"command", "volumes"}}
    for changed in ({"networks": {"collector": {}}}, {"ports": [8100]}, {"environment": {"TUNNEL_TOKEN": "fixture"}},
                    {"image": "cloudflared:latest"}, {"read_only": False}, {"mem_limit": 268435456}):
        bad = copy.deepcopy(original)
        bad["services"]["tunnel"].update(changed)
        with pytest.raises(ValueError):
            installer.remote_tunnel_spec(bad)
