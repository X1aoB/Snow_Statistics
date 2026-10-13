"""Synthetic transport/catalog responses; no VM or Hive integration claims."""
import io
import json
import os
import subprocess
import sys
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_real_lab import config

from snow_statistics import real_hive_dispatch as module
from snow_statistics.io import digest, write_json
from snow_statistics.publication import canonical
from snow_statistics.real_hive import HiveRegistry, SparkCatalog, catalog_runner_context, run_catalog_worker
from snow_statistics.real_remote_lifecycle import RealRemoteLifecycle


def source_root(path):
    for name in module.ENGINE_FILES:
        target = path / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("synthetic source identity " + name)
    (path / "lab/.env").write_text("CONTROL_IP=192.168.216.130\n")
    return path


def envelope(root, *, now=None):
    now = now or datetime.now(UTC)
    request = dict(schema_version=1, action="cleanup", metastore_uri="thrift://192.168.216.130:9083",
                   owner_sha256="a" * 64, tables={}, known_tables=[], requested_at=now.isoformat())
    return dict(schema_version=1, kind="catalog", session="b" * 64, sequence=1, lane="prod01", request=request,
                request_sha256=digest(canonical(request)), engine=module.engine_identity(root),
                authority_sha256="c" * 64, expires_at=(now + timedelta(minutes=9)).isoformat())


def reply(request):
    spec = request["request"]
    result = dict(schema_version=1, source="real", action=spec["action"], tables={},
                  request_sha256=request["request_sha256"], engine="Spark 3.5.7", master="local[1]",
                  application_id="local-123456789", metastore_uri=spec["metastore_uri"], checked_at=datetime.now(UTC).isoformat())
    return {key: request[key] for key in ("schema_version", "session", "sequence", "request_sha256", "engine")} | {
        "kind": "catalog-result", "result": result}


def test_context_reaches_new_script_threads_and_restores_plain_cli(tmp_path):
    registry = SimpleNamespace()
    def remote(*args, **kwargs):
        return None
    observed = []
    with catalog_runner_context(remote):
        thread = threading.Thread(target=lambda: observed.append(SparkCatalog(tmp_path, registry, "192.168.216.130").runner))
        thread.start()
        thread.join()
        with pytest.raises(ValueError, match="already active"):
            with catalog_runner_context(remote):
                pass
    assert observed == [remote]
    assert SparkCatalog(tmp_path, registry, "192.168.216.130").runner is run_catalog_worker
    with pytest.raises(RuntimeError):
        with catalog_runner_context(remote):
            raise RuntimeError("synthetic process failure")
    assert SparkCatalog(tmp_path, registry, "192.168.216.130").runner is run_catalog_worker


@pytest.mark.parametrize("kind", ["empty", "incomplete", "noncanonical", "extra-frame", "oversize"])
def test_protocol_does_not_accept_arbitrary_or_unbounded_success_json(kind):
    body = {"empty": b"", "incomplete": b"{}", "noncanonical": b'{ "ok": true }\n',
            "extra-frame": b"{}\nextra"}.get(kind)
    if kind == "oversize":
        body = b"x" * (module.MAX_FRAME + 2)
    if body == b"{}\nextra":
        stream = io.BytesIO(body)
        assert module.read_frame(stream) == {}
        with pytest.raises(ValueError):
            module.read_frame(stream)
    else:
        with pytest.raises((ValueError, UnicodeDecodeError)):
            module.read_frame(io.BytesIO(body))


@pytest.mark.parametrize("mutation", [
    lambda v: v.update(sequence=True), lambda v: v.update(lane="../../business"),
    lambda v: v.update(kind="success"), lambda v: v.update(request_sha256="f" * 64),
    lambda v: v.update(expires_at=(datetime.now(UTC) - timedelta(seconds=1)).isoformat()),
    lambda v: v.update(expires_at=(datetime.now(UTC) + timedelta(hours=1)).isoformat()),
    lambda v: v["engine"].update(unregistered="d" * 64),
])
def test_request_binding_deadline_and_source_fail_closed(tmp_path, mutation):
    value = envelope(source_root(tmp_path))
    mutation(value)
    with pytest.raises(ValueError):
        module.validate_envelope(value)


@pytest.mark.parametrize("field,value", [("session", "f" * 64), ("sequence", 2), ("request_sha256", "f" * 64)])
def test_a_previous_live_response_cannot_answer_another_request(tmp_path, field, value):
    request = envelope(source_root(tmp_path))
    response = reply(request)
    response[field] = value
    with pytest.raises(ValueError):
        module.validate_reply(response, request)


def test_control_runs_fresh_fixed_worker_and_never_reuses_result_file(tmp_path):
    root = source_root(tmp_path)
    request = envelope(root)
    calls, stopped = [], []
    def actual(command, **kwargs):
        calls.append((command, kwargs))
        assert command[:2] == ["bash", "tools/spark_hive_catalog.sh"]
        assert 0 < kwargs["timeout"] <= 420
        stored = json.loads((root / command[2]).read_bytes())
        assert stored == request["request"]
        return SimpleNamespace(returncode=0, stdout="SNOW_HIVE_RESULT=" + json.dumps(reply(request)["result"]) + "\n")
    value = module.run_control(request, root, runner=actual, stop=lambda *args: stopped.append(args))
    assert module.validate_reply(value, request)["engine"] == "Spark 3.5.7"
    assert len(calls) == len(stopped) == 1
    assert not (root / calls[0][0][2]).exists()
    with pytest.raises(ValueError, match="replay"):
        module.run_control(request, root, runner=actual, stop=lambda *args: None)
    assert len(calls) == 1


@pytest.mark.parametrize("entry", ["windows", "authority", "control"])
def test_registered_copy_cleanup_precedes_new_reads_and_failure_opens_no_transport(tmp_path, entry):
    from snow_statistics.real_lake_authority import NAMESPACE
    root = source_root(tmp_path)
    configuration = config()
    configuration["lane"] = "prod01"
    configuration["nodes"]["snow-control"] = "192.168.216.130"
    private = "runtime/real/config/test.json"
    write_json(root / private, configuration)
    (root / private).chmod(0o600)
    # Real bounded inventory failure, not a fabricated adapter success/failure.
    unexpected = root / NAMESPACE / "unregistered-payload.json"
    unexpected.parent.mkdir(parents=True)
    unexpected.write_text("synthetic fixture")
    calls = []
    transport = SimpleNamespace(start=lambda *args, **kwargs: calls.append(args))
    with pytest.raises(ValueError, match="Unregistered lake namespace"):
        if entry == "windows":
            module.HiveCoordinator(configuration, private, root, transport=transport).run("cleanup")
        elif entry == "authority":
            module.authority(root, private, "cleanup", None, None, None, 900, io.BytesIO(), io.BytesIO())
        else:
            module.run_control(envelope(root), root, runner=lambda *args, **kwargs: calls.append(args))
    assert not calls and unexpected.exists()
    assert not (root / "runtime/real/hive-dispatch").exists()


@pytest.mark.parametrize("failure", ["source", "target", "exit", "missing", "duplicate", "future", "throw"])
def test_control_rejects_fake_engine_and_failure_always_stops_exact_attempt(tmp_path, failure):
    root = source_root(tmp_path)
    request = envelope(root)
    calls, stopped = [], []
    if failure == "source":
        (root / module.ENGINE_FILES[0]).write_text("different source")
    if failure == "target":
        (root / "lab/.env").write_text("CONTROL_IP=192.168.216.199\n")
    def actual(*args, **kwargs):
        calls.append(args)
        if failure == "throw":
            raise TimeoutError("synthetic driver timeout")
        result = reply(request)["result"]
        if failure == "future":
            result["checked_at"] = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
        line = "SNOW_HIVE_RESULT=" + json.dumps(result) + "\n"
        return SimpleNamespace(returncode=1 if failure == "exit" else 0,
                               stdout="" if failure == "missing" else line * (2 if failure == "duplicate" else 1))
    with pytest.raises((ValueError, TimeoutError)):
        module.run_control(request, root, runner=actual, stop=lambda *args: stopped.append(args))
    assert len(stopped) == (0 if failure in {"source", "target"} else 1)


def test_expired_cancellation_checks_original_reservation_and_exact_container_id(tmp_path):
    root = source_root(tmp_path)
    # A failed copy inventory closes new reads, never exact owned cancellation.
    invalid_copy = root / "runtime/real/lake-authority/unregistered-payload.json"
    invalid_copy.parent.mkdir(parents=True)
    invalid_copy.write_text("synthetic fixture")
    request = envelope(root, now=datetime.now(UTC) - timedelta(hours=1))
    name = "snow-real-hive-" + request["request_sha256"][:20]
    record = module._worker_record(root, request)
    write_json(record, dict(request=request, name=name))
    cid = "d" * 64
    cidfile = module._controlled_path(root, f"runtime/real/lifecycle/prod01/hive-requests/{request['request_sha256']}.cid")
    cidfile.write_text(cid)
    calls = []
    def docker(command, **kwargs):
        calls.append(command)
        if "stop" in command:
            assert command[-1] == cid
            return SimpleNamespace(returncode=0)
        running = "true" if len(calls) == 1 else "false"
        return SimpleNamespace(returncode=0, stdout=f"{cid}|/{name}|{running}|{request['request_sha256']}\n".encode(), stderr=b"")
    with pytest.raises(ValueError):
        module.validate_envelope(request)
    module.stop_worker(root, request, run=docker)
    assert len(calls) == 3
    def foreign(*args, **kwargs):
        return SimpleNamespace(returncode=0, stdout=b"unrelated\n", stderr=b"")
    with pytest.raises(ValueError, match="replaced"):
        module.stop_worker(root, request, run=foreign)


def test_short_permission_never_launches_a_driver_and_longer_one_is_clamped(tmp_path):
    root = source_root(tmp_path)
    request = envelope(root)
    request["expires_at"] = (datetime.now(UTC) + timedelta(seconds=60)).isoformat()
    launches, stopped = [], []
    def actual(*args, **kwargs):
        launches.append(kwargs["timeout"])
        raise TimeoutError("synthetic deadline termination")
    with pytest.raises(ValueError, match="Insufficient"):
        module.run_control(request, root, runner=actual, stop=lambda *args: stopped.append(args))
    assert not launches and not stopped
    request["expires_at"] = (datetime.now(UTC) + timedelta(seconds=200)).isoformat()
    with pytest.raises(TimeoutError):
        module.run_control(request, root, runner=actual, stop=lambda *args: stopped.append(args))
    assert len(launches) == len(stopped) == 1 and 0 < launches[0] <= 50


def test_capacity_rejection_cannot_block_exact_cancel_ssh(tmp_path, monkeypatch):
    transport = object.__new__(module.NodeTransport)
    transport.root = tmp_path
    calls = []
    def full(*args):
        raise ValueError("synthetic project capacity exhausted")
    transport.vmware = SimpleNamespace(capacity=full, RUNTIME=tmp_path, VMWARE=tmp_path,
                                      guest_ip=lambda *args: "192.168.216.130")
    monkeypatch.setattr(module.os, "name", "nt")
    monkeypatch.setattr(module.subprocess, "CREATE_NO_WINDOW", 0, raising=False)
    monkeypatch.setattr(module.subprocess, "Popen", lambda command, **kwargs: calls.append(command))
    with pytest.raises(ValueError, match="capacity"):
        transport.start("snow-control", ["--node-phase", "worker"])
    transport.start("snow-control", ["--node-phase", "cancel"], reserve=False)
    assert len(calls) == 1 and calls[0][-1].endswith("--node-phase cancel")
    with pytest.raises(ValueError, match="Only exact"):
        transport.start("snow-control", ["--node-phase", "worker"], reserve=False)


def test_windows_worker_timeout_is_bounded_by_the_same_original_permission(tmp_path):
    request = envelope(source_root(tmp_path))
    request["expires_at"] = (datetime.now(UTC) + timedelta(seconds=200)).isoformat()
    class Process(FakeProcess):
        def communicate(self, body, *, timeout):
            assert 0 < timeout <= 185
            raise subprocess.TimeoutExpired("synthetic owned SSH", timeout)
    process = Process([])
    transport = object.__new__(module.NodeTransport)
    transport.start = lambda *args, **kwargs: process
    with pytest.raises(subprocess.TimeoutExpired):
        transport.worker(request)
    assert process.returncode == -15


def test_authority_uses_canonical_local_request_and_checks_registry_after_readback(tmp_path):
    root = source_root(tmp_path)
    configuration = config()
    configuration["nodes"]["snow-control"] = "192.168.216.130"
    configuration["lane"] = "prod01"
    manager = RealRemoteLifecycle(root / "runtime/real/lifecycle/prod01")
    prefix = "hdfs://192.168.216.130:9000/snow/"
    manager.initialize(prefix + "warehouse/real/prod01", prefix + "auxiliary/real/prod01",
                       prefix + "ods/real/kafka/prod01", "00000000-0000-4000-8000-000000000001",
                       "00000000-0000-4000-8000-000000000002")
    output = io.BytesIO()
    changed = False
    class Incoming:
        def next(self, timeout):
            value = json.loads(output.getvalue().splitlines()[-1])
            if changed:
                manager.register(prefix + "warehouse/real/prod01/extra", "aggregate", datetime.now(UTC).isoformat())
            return reply(value)
    runner = module.AuthorityRunner(root, configuration, Incoming(), output, "b" * 64, datetime.now(UTC) + timedelta(minutes=15))
    with catalog_runner_context(runner):
        catalog = SparkCatalog(root, HiveRegistry(manager), configuration["nodes"]["snow-control"])
        assert catalog.execute("cleanup", {}, datetime.now(UTC))["tables"] == {}
        changed = True
        with pytest.raises(ValueError, match="Authority"):
            catalog.execute("cleanup", {}, datetime.now(UTC))
    assert runner.closed
    assert not list((manager.directory / "hive-requests").glob("*.json"))


class FakeProcess:
    def __init__(self, frames):
        self.stdout = io.BytesIO(b"".join(module.frame_bytes(frame) for frame in frames))
        self.stdin, self.returncode = io.BytesIO(), None
    def poll(self):
        return self.returncode
    def wait(self, **unused):
        # Completion must not close stdin first: the authority's disconnect
        # watchdog would otherwise race its own normal finally/exit handling.
        assert not self.stdin.closed
        self.returncode = 0 if self.returncode is None else self.returncode
        return self.returncode
    def terminate(self):
        self.returncode = -15
    def kill(self):
        self.returncode = -9


@pytest.mark.parametrize("fails", [False, True, "terminate_error"])
def test_windows_coordinator_dispatches_actual_control_call_and_cancels_on_failure(tmp_path, fails, monkeypatch):
    root = source_root(tmp_path)
    value = config()
    value["lane"] = "prod01"
    value["nodes"]["snow-control"] = "192.168.216.130"
    request = envelope(root)
    ready = dict(schema_version=1, kind="ready", session=request["session"], operation="cleanup",
                 expires_at=(datetime.now(UTC) + timedelta(seconds=899)).isoformat())
    completed = dict(schema_version=1, kind="complete", session=request["session"], operation="cleanup", result={"source": "real"})
    process = FakeProcess([ready, request, completed])
    calls = []
    class Transport:
        def start(self, node, arguments, **kwargs):
            assert node == "snow-analysis" and arguments[:2] == ["--node-phase", "authority"]
            return process
        def worker(self, actual, *, cancel=False):
            calls.append((actual, cancel))
            if fails and not cancel:
                raise RuntimeError("synthetic SSH disconnect")
            return {"stopped": True} if cancel else reply(actual)
    coordinator = module.HiveCoordinator(value, "runtime/real/config/test.json", root, transport=Transport())
    if fails == "terminate_error":
        def terminate_error(*unused):
            raise OSError("synthetic local SSH termination failed")
        monkeypatch.setattr(module, "terminate", terminate_error)
    if fails:
        with pytest.raises(OSError if fails == "terminate_error" else RuntimeError):
            coordinator.run("cleanup")
        assert [cancel for _, cancel in calls] == [False, True]
    else:
        assert coordinator.run("cleanup") == {"source": "real"}
        assert [cancel for _, cancel in calls] == [False]
    if fails != "terminate_error":
        assert process.poll() is not None
    assert process.stdin.closed and process.stdout.closed


@pytest.mark.parametrize("args", [("all", None, None, None), ("cleanup", "run", None, None),
                                  ("view", None, None, None), ("register", "run;id", None, None),
                                  ("lake-confirm", "run", "attempt", None), ("lake-prepare", "run", "../escape", None)])
def test_no_unknown_phase_shell_or_success_receipt_entry(args):
    with pytest.raises(ValueError):
        module.operation_arguments(*args)


def test_frozen_writer_files_are_not_needed_for_the_explicit_context():
    from snow_statistics.real_quiescent import WRITER_FILES
    assert "src/snow_statistics/real_hive.py" not in WRITER_FILES
    assert "src/snow_statistics/real_hive_dispatch.py" not in WRITER_FILES
    tool = Path(__file__).resolve().parents[1] / "tools/real_hive_dispatch.py"
    result = subprocess.run([sys.executable, str(tool), "--help"], capture_output=True, text=True, check=True)
    assert "--operation" in result.stdout and "success-json" not in result.stdout


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX process-group behavior is checked on Linux CI")
def test_real_local_process_boundary_rejects_unbounded_output_and_timeout(tmp_path, monkeypatch):
    monkeypatch.setattr(module, "MAX_FRAME", 100)
    with pytest.raises((ValueError, TimeoutError)):
        module.bounded_catalog_worker([sys.executable, "-c", "print('x' * 1000)"], cwd=tmp_path, timeout=5)
    with pytest.raises(TimeoutError):
        module.bounded_catalog_worker([sys.executable, "-c", "import time; time.sleep(5)"], cwd=tmp_path, timeout=.01)


@pytest.mark.skipif(sys.platform == "win32", reason="Authority executes only on the Linux analysis node")
@pytest.mark.parametrize("partial", [b"", b'{"unfinished":'])
def test_pipe_reader_stops_even_when_peer_keeps_an_empty_or_partial_frame_open(partial):
    read_fd, write_fd = os.pipe()
    with os.fdopen(read_fd, "rb") as incoming, os.fdopen(write_fd, "wb", buffering=0) as outgoing:
        reader = module.FrameReader(incoming)
        try:
            outgoing.write(partial)
            with pytest.raises(TimeoutError):
                reader.next(.05)
        finally:
            reader.close()
        assert not reader.thread.is_alive() and reader.ended.is_set()
        assert not outgoing.closed


@pytest.mark.skipif(sys.platform == "win32", reason="Linux raw pipe parsing")
def test_pipe_reader_keeps_split_frames_and_rejects_unbounded_input(monkeypatch):
    monkeypatch.setattr(module, "MAX_FRAME", 64)
    read_fd, write_fd = os.pipe()
    with os.fdopen(read_fd, "rb") as incoming, os.fdopen(write_fd, "wb", buffering=0) as outgoing:
        reader = module.FrameReader(incoming)
        try:
            outgoing.write(b'{"n":')
            with pytest.raises(TimeoutError):
                reader.next(.05)
            outgoing.write(b'1}\n{"n":2}\n')
            assert reader.next(2) == {"n": 1}
            assert reader.next(2) == {"n": 2}
            outgoing.write(b"x" * 66)
            with pytest.raises(ValueError, match="transport closed"):
                reader.next(2)
        finally:
            reader.close()
        assert len(reader.buffer) <= module.MAX_FRAME + 2


def test_reader_queue_overflow_closes_instead_of_accumulating_frames():
    reader = module.FrameReader(io.BytesIO(b'{}\n' * 8))
    assert reader.ended.wait(2)
    with pytest.raises(ValueError, match="closed"):
        reader.next(1)
    reader.close()
    assert reader.queue.qsize() == 2


@pytest.mark.skipif(sys.platform == "win32", reason="Actual authority and signal lifecycle run on Linux")
@pytest.mark.parametrize("case", ["complete", "partial", "failure", "disconnect"])
def test_authority_real_process_exit_with_peer_still_open_or_disconnected(tmp_path, case):
    """Synthetic operation; exercise real stdin locks, threads and exit status."""
    root = source_root(tmp_path)
    private = "runtime/real/config/test.json"
    write_json(root / private, config())
    (root / private).chmod(0o600)
    operation = {
        "complete": "return {'source': 'real'}",
        "partial": "return {'source': 'real'}",
        "failure": "raise RuntimeError('synthetic operation failure')",
        "disconnect": "import time; time.sleep(60); return {'source': 'real'}",
    }[case]
    (root / "tools/real_hive.py").write_text("def execute(*args, **kwargs):\n    " + operation + "\n")
    script = """
import signal, sys
from pathlib import Path
from snow_statistics.real_hive_dispatch import authority
def stopped(*_):
    raise SystemExit(143)
signal.signal(signal.SIGTERM, stopped)
authority(Path(sys.argv[1]), 'runtime/real/config/test.json', 'cleanup', None,
          None, None, 60, sys.stdin.buffer, sys.stdout.buffer)
"""
    process = subprocess.Popen([sys.executable, "-u", "-c", script, str(root)],
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    reader = module.FrameReader(process.stdout)
    try:
        if case == "partial":
            process.stdin.write(b'{"unfinished":')
            process.stdin.flush()
        ready = reader.next(5)
        assert ready["kind"] == "ready"
        if case == "disconnect":
            process.stdin.close()
        if case in {"complete", "partial"}:
            assert reader.next(5)["kind"] == "complete"
        result = process.wait(timeout=5)
        assert result == (0 if case in {"complete", "partial"} else 143 if case == "disconnect" else 1)
        assert b"_enter_buffered_busy" not in process.stderr.read()
        if case != "disconnect":
            assert not process.stdin.closed
    finally:
        module.terminate(process)
        reader.close()
        for stream in (process.stdin, process.stdout, process.stderr):
            if not stream.closed:
                stream.close()


def test_complete_frame_does_not_override_nonzero_authority_exit(tmp_path):
    ready = dict(schema_version=1, kind="ready", session="b" * 64, operation="cleanup",
                 expires_at=(datetime.now(UTC) + timedelta(seconds=899)).isoformat())
    complete = dict(schema_version=1, kind="complete", session="b" * 64,
                    operation="cleanup", result={"source": "real"})
    process = FakeProcess([ready, complete])
    process.returncode = 71
    transport = SimpleNamespace(start=lambda *args, **kwargs: process)
    with pytest.raises(RuntimeError, match="did not complete"):
        module.HiveCoordinator(config(), "runtime/real/config/test.json", tmp_path,
                               transport=transport).run("cleanup")
    assert process.returncode == 71
