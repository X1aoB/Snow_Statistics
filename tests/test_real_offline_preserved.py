"""Synthetic engine adapters only: compatibility is not a measured VM receipt."""
import copy
import io
import json
import signal
from contextlib import nullcontext
from datetime import timedelta
from types import SimpleNamespace

import pytest
from test_real_offline_small import FixtureRunner, config
from test_real_quiescent import COLLECTOR, NOW, FixtureDocker, FixtureProbe
from test_real_writer_recovery import Probe

from snow_statistics import real_offline_preserved as preserved
from snow_statistics import real_offline_small as small
from snow_statistics.io import digest, write_json
from snow_statistics.publication import canonical
from snow_statistics.real_epoch import Epoch, make_manifest, prepare
from snow_statistics.real_quiescent import WriterRegistry
from snow_statistics.real_writer_recovery import WriterRecovery


def current_config():
    return config() | {"lane": preserved.CURRENT}


def candidate(root):
    jar = root / "fixture.jar"
    jar.write_bytes(b"synthetic not an engine")
    images = {name: "fixture@sha256:" + "a" * 64 for name in ("KAFKA_IMAGE", "DORIS_FE_IMAGE", "DORIS_BE_IMAGE", "FLINK_IMAGE")}
    docker, registries = FixtureDocker(), {}
    for lane in (preserved.FAILED, preserved.CURRENT):
        folder = root / "runtime/real/epochs" / lane
        manifest = make_manifest(lane, (NOW - timedelta(hours=1)).isoformat(), images, now=NOW)
        prepare(folder, manifest, "192.168.100.103", jar)
        epoch = Epoch(folder, docker)
        docker.start(folder / "compose.json", list(manifest["containers"]))
        registry, probe = WriterRegistry(epoch), FixtureProbe(manifest)
        registry.initialize(COLLECTOR, probe, now=NOW)
        registry.record_job("a" * 32, probe, now=NOW)
        write_json(root / "runtime/real/sync" / lane / "source.json", COLLECTOR)
        if lane == preserved.CURRENT:
            writer = SimpleNamespace(registry=registry, epoch=epoch, identity=lambda: COLLECTOR,
                                     manifest=manifest, directory=folder / "writer")
            clock = lambda: NOW + timedelta(seconds=1)  # noqa: E731
            WriterRecovery(writer, probe=Probe(registry, clock), clock=clock).pause()
        else:
            epoch.stop()
        registries[lane] = registry
    sync = root / "runtime/real/sync" / preserved.FAILED
    for name, value in {"target.json": dict(source="real", lane="real_prod_03"), "cursor.json": dict(cursor=201),
                        "rebase.json": dict(source=COLLECTOR, after=201, new_lane="real_prod_03", coverage="starts_after_explicit_gap")}.items():
        write_json(sync / name, value)
    for relative in ("epochs/real-prod-03", "epochs/real-prod-03/writer-operation", "sync/real-prod-03",
                     "epochs/real-prod-04/writer-operation"):
        folder = root / "runtime/real" / relative
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "publisher.lock").touch()
    return docker, registries


def observed(root, docker):
    return preserved.observed_state(current_config(), root, docker, now=NOW + timedelta(seconds=2))


def test_candidate_has_no_pause_retirement_or_kafka_empty_claim(tmp_path):
    docker, registries = candidate(tmp_path)
    before = {path: path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()}
    state = observed(tmp_path, docker)
    assert state["epochs"][preserved.FAILED]["state"] == "abandoned_stopped"
    assert state["epochs"][preserved.CURRENT]["state"] == "paused"
    assert state["failed_local_input_chain_absent"] and not state["kafka_empty_claimed"]
    assert not state["read_permission"] and not state["retirement_or_recovery_created"]
    assert all(path.read_bytes() == data for path, data in before.items())
    assert registries[preserved.FAILED].recovery(now=NOW + timedelta(seconds=2)) == []


@pytest.mark.parametrize("wrong", ["payload", "pending", "cursor", "source", "ods", "running", "recreated", "not_paused", "expired", "another_epoch", "other_current"])
def test_preservation_refuses_changes_and_never_removes_them(tmp_path, wrong):
    docker, registries = candidate(tmp_path)
    sync = tmp_path / "runtime/real/sync" / preserved.FAILED
    at, chosen = NOW + timedelta(seconds=2), current_config()
    if wrong in {"payload", "pending"}:
        write_json(sync / ("data.json" if wrong == "payload" else "pending.json"), {})
    elif wrong == "cursor":
        write_json(sync / "cursor.json", {"cursor": 202})
    elif wrong == "source":
        write_json(sync / "source.json", COLLECTOR | {"instance_id": "00000000-0000-0000-0000-000000000003"})
    elif wrong == "ods":
        (tmp_path / "runtime/real/ods" / preserved.FAILED).mkdir(parents=True)
    elif wrong == "running":
        next(iter(docker.c.values()))["running"] = True
    elif wrong == "recreated":
        next(iter(docker.v.values()))["CreatedAt"] = "replaced"
    elif wrong == "not_paused":
        for path in (registries[preserved.CURRENT].epoch.directory / "writer-recovery").glob("*.json"):
            path.unlink()
    elif wrong == "expired":
        at += timedelta(days=7)
    elif wrong == "another_epoch":
        (tmp_path / "runtime/real/epochs/unregistered").mkdir()
    else:
        chosen["lane"] = "another-current"
    before = {path: path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()}
    with pytest.raises((ValueError, FileNotFoundError)):
        preserved.observed_state(chosen, tmp_path, docker, now=at)
    assert all(path.read_bytes() == body for path, body in before.items())


def test_missing_state_is_never_initialized(tmp_path):
    with pytest.raises(FileNotFoundError), preserved.scope_locks(tmp_path):
        pass
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("lane,checksum", [(None, "a" * 64), (preserved.FAILED, None), ("other", "a" * 64), (preserved.FAILED, "../secret")])
def test_selection_cannot_be_broadened_or_silently_enabled(lane, checksum):
    assert preserved.selection(None, None) is None
    with pytest.raises(ValueError):
        preserved.selection(lane, checksum)


def test_ssh_only_routes_explicit_digest_to_analysis(tmp_path):
    runner = FixtureRunner(tmp_path)
    plain = runner.ssh("snow-analysis", "guard")[-1]
    assert "--preserve-stopped-epoch" not in plain
    runner.preserved = preserved.selection(preserved.FAILED, "d" * 64)
    assert "--preserve-stopped-epoch real-prod-03" in runner.ssh("snow-analysis", "guard")[-1]
    assert "--preserved-state-sha256 " + "d" * 64 in runner.ssh("snow-analysis", "execute", phase="node-start-offline")[-1]
    assert "--preserve-stopped-epoch" not in runner.ssh("snow-control", "execute", phase="node-start-offline")[-1]


def test_existing_controller_guards_and_failure_cleanup_remain(tmp_path):
    runner = FixtureRunner(tmp_path)
    runner.preserved = preserved.selection(preserved.FAILED, "d" * 64)
    runner.failure = ("node-start-offline", "snow-analysis")
    with pytest.raises(RuntimeError):
        runner.perform("start-offline")
    assert not any(runner.state.values())
    assert ("guard", "snow-analysis") in runner.calls
    assert any(call[0] == "stop" for call in runner.calls)


def identity():
    return dict(container_id="a" * 64, ownership_sha256="b" * 64, created_at=NOW.isoformat(),
                service_sha256="c" * 64, volumes_sha256="d" * 64)


def test_stop_journal_is_exact_id_idempotent_and_does_not_require_live_epoch(tmp_path, monkeypatch):
    worker = dict(synthetic=True)
    directory = tmp_path / "attempt"
    scope = dict(schema_version=1, worker_sha256=digest(canonical(worker)), identity=identity())
    write_json(directory / "preserved-service.json", scope)
    calls, running = [], [True]
    class Docker:
        def command(self, args, **kwargs):
            calls.append(args)
            if args[0] == "stop":
                running[0] = False
                return b""
            assert args == ["inspect", "a" * 64]
            return json.dumps([{"State": {"Running": running[0]}}]).encode()
    def actual(root, docker, *, running):
        return identity()
    monkeypatch.setattr(preserved, "datanode_identity", actual)
    assert preserved.stop_service(tmp_path, directory, worker, docker=Docker())["status"] == "stopped"
    assert ["stop", "--time", "15", "a" * 64] in calls
    running[0] = True  # A later admitted attempt now owns the same preserved DN.
    count = len(calls)
    assert preserved.stop_service(tmp_path, directory, worker, docker=Docker())["status"] == "already_stopped_by_this_attempt"
    assert len(calls) == count and running[0]


@pytest.mark.parametrize("wrong", ["replaced", "disconnect", "invalid_id", "worker"])
def test_cancel_does_not_claim_cleanup_for_wrong_object_or_unknown_daemon(tmp_path, monkeypatch, wrong):
    worker, directory = dict(synthetic=True), tmp_path / "attempt"
    selected = identity()
    if wrong == "invalid_id":
        selected["container_id"] = "--all"
    write_json(directory / "preserved-service.json", dict(schema_version=1,
               worker_sha256="e" * 64 if wrong == "worker" else digest(canonical(worker)), identity=selected))
    calls = []
    def query(args, **kwargs):
        calls.append(args)
        if wrong == "disconnect":
            raise RuntimeError("synthetic Docker failure")
        return b'[{"State":{"Running":true}}]'
    monkeypatch.setattr(preserved, "datanode_identity", lambda *_, **__: identity() | {"container_id": "f" * 64})
    with pytest.raises((ValueError, RuntimeError)):
        preserved.stop_service(tmp_path, directory, worker, docker=SimpleNamespace(command=query))
    assert not (directory / "preserved-service-stopped.json").exists()
    assert not any(call[0] == "stop" for call in calls)
    if wrong in {"invalid_id", "worker"}:
        assert calls == []


def test_preserved_child_keeps_existing_heartbeat_and_cleanup_on_disconnect(tmp_path, monkeypatch):
    cleaned, stopped = [], []
    chosen = preserved.selection(preserved.FAILED, "d" * 64)
    class Process:
        pid, returncode = 1234, None
        stdout = io.BytesIO(b"synthetic metadata")
        def __init__(self, command, **kwargs):
            assert command[1] == "tools/real_offline_preserved.py"
            assert "--preserved-state-sha256" in command and kwargs["start_new_session"]
        def poll(self):
            return self.returncode
    monkeypatch.setattr(small.subprocess, "Popen", Process)
    monkeypatch.setattr(small, "process_identity", lambda pid: dict(pid=pid, start_ticks="1", argv_sha256="a" * 64))
    monkeypatch.setattr(small, "child_session", lambda pid: dict(pid=pid, pgid=pid, sid=pid, start_ticks="1"))
    def stop_session(session, process=None):
        stopped.append(session["pid"])
        return dict(active_members_after_cleanup=0, kill_escalated=False)
    monkeypatch.setattr(small, "stop_session", stop_session)
    monkeypatch.setattr(small.select, "select", lambda *a: ([123], [], []))
    monkeypatch.setattr(small.os, "read", lambda fd, size: b"")
    monkeypatch.setattr(preserved, "stop_service", lambda root, directory, worker: cleaned.append(copy.deepcopy(worker)))
    with pytest.raises(RuntimeError, match="heartbeat"):
        small.node_execute(current_config(), "runtime/real/config/test.json", tmp_path, "node-start-offline", None,
                           "a" * 32, SimpleNamespace(fileno=lambda: 123), preserved=chosen)
    assert stopped == [1234] and cleaned[0]["preserved_failed"] == chosen
    assert json.loads((small.attempt_path(tmp_path, "a" * 32) / "done.json").read_bytes())["phase_complete"] is False


@pytest.mark.parametrize("fail", [None, "before_scope", "start_command", "changed_after_start"])
def test_start_durably_records_only_existing_id_and_closes_failed_start(tmp_path, monkeypatch, fail):
    from snow_statistics import real_transfer
    directory = small.attempt_path(tmp_path, "a" * 32)
    directory.mkdir(parents=True)
    state, calls, reads = {"synthetic": True}, [], [0]
    chosen = preserved.selection(preserved.FAILED, digest(canonical(state)))
    worker = dict(config_sha256=digest(canonical(current_config())), phase="node-start-offline", run_id=None,
                  process={"pid": 123}, preserved_failed=chosen)
    write_json(directory / "worker.json", worker)
    def audit(*args):
        reads[0] += 1
        if fail == "before_scope":
            raise ValueError("synthetic scope change")
        return {"changed": True} if fail == "changed_after_start" and reads[0] > 1 else state
    def command(args, **kwargs):
        calls.append(args)
        if args[0] == "start":
            # This real durable file must exist before the mutation is called.
            recorded = json.loads((directory / "preserved-service.json").read_bytes())
            assert recorded["identity"] == identity() and recorded["worker_sha256"] == digest(canonical(worker))
            if fail == "start_command":
                raise RuntimeError("synthetic start timeout")
    monkeypatch.setattr(preserved, "checked_worker", lambda *args: worker)
    monkeypatch.setattr(preserved, "expire_due", lambda *args: None)
    monkeypatch.setattr(preserved, "scope_locks", lambda *args: nullcontext())
    monkeypatch.setattr(preserved, "observed_state", audit)
    monkeypatch.setattr(preserved, "datanode_identity", lambda *args, **kwargs: identity())
    monkeypatch.setattr(preserved, "stop_service", lambda *args, **kwargs: calls.append(["cleanup", "a" * 64]))
    monkeypatch.setattr(real_transfer, "cleanup_copies", lambda *args: None)
    monkeypatch.setattr(small, "guest_sample", lambda *args: dict(node="snow-analysis", available_ram_mib=256, disk_free_mib=512, containers={}))
    docker = SimpleNamespace(command=command)
    if fail:
        with pytest.raises((ValueError, RuntimeError)):
            preserved.start_service(current_config(), tmp_path, chosen, "a" * 32, docker=docker)
    else:
        result = preserved.start_service(current_config(), tmp_path, chosen, "a" * 32, docker=docker)
        assert result["offline_services"] == "up" and not result["lifecycle_permit"]
    if fail == "before_scope":
        assert calls == [] and not (directory / "preserved-service.json").exists()
    else:
        assert calls[0] == ["start", "a" * 64]
        assert (["cleanup", "a" * 64] in calls) is bool(fail)


def test_no_unsupervised_child_or_cancelled_parent_can_start(tmp_path, monkeypatch):
    chosen = preserved.selection(preserved.FAILED, "d" * 64)
    worker = dict(config_sha256=digest(canonical(current_config())), phase="node-start-offline", run_id=None,
                  process={"pid": 123, "start_ticks": "1", "argv_sha256": "a" * 64}, preserved_failed=chosen)
    write_json(tmp_path / "worker.json", worker)
    monkeypatch.setattr(preserved.os, "getppid", lambda: 999)
    monkeypatch.setattr(small, "process_identity", lambda pid: worker["process"])
    with pytest.raises(ValueError, match="parent"):
        preserved.checked_worker(tmp_path, current_config(), chosen)
    monkeypatch.setattr(preserved.os, "getppid", lambda: 123)
    assert preserved.checked_worker(tmp_path, current_config(), chosen) == worker
    write_json(tmp_path / "cancelled.json", {})
    with pytest.raises(ValueError, match="parent"):
        preserved.checked_worker(tmp_path, current_config(), chosen)


def test_cancel_recovers_daemon_scope_even_after_worker_already_exited(tmp_path, monkeypatch):
    directory = small.attempt_path(tmp_path, "a" * 32)
    chosen = preserved.selection(preserved.FAILED, "d" * 64)
    worker = dict(config_sha256=digest(canonical(current_config())), phase="node-start-offline", run_id=None,
                  process={"pid": 123, "start_ticks": "1", "argv_sha256": "a" * 64}, preserved_failed=chosen)
    write_json(directory / "worker.json", worker)
    child = dict(schema_version=1, attempt="a" * 32, worker_sha256=digest(canonical(worker)), command_sha256="b" * 64,
                 session=dict(pid=124, start_ticks="2", pgid=124, sid=124))
    write_json(directory / "child.json", child)
    cleaned = []
    monkeypatch.setattr(small, "process_identity", lambda pid: None)
    monkeypatch.setattr(small, "stop_session", lambda *args: dict(active_members_after_cleanup=0, kill_escalated=False))
    monkeypatch.setattr(preserved, "stop_service", lambda root, directory, worker: cleaned.append(worker))
    result = small.node_cancel(tmp_path, current_config(), "a" * 32)
    assert result["status"] == "cancelled" and cleaned == [worker]
    done = json.loads((directory / "done.json").read_bytes())
    assert done["child_group_stopped"] and done["driver_stopped"] and not done["phase_complete"]


@pytest.mark.parametrize("last_event", ["eof", "cancel_after_result", "success"])
def test_exited_child_still_needs_fresh_controller_before_success(tmp_path, monkeypatch, last_event):
    chosen, cleaned = preserved.selection(preserved.FAILED, "d" * 64), []
    class Process:
        pid, returncode = 1234, 0
        stdout = io.BytesIO(b'{"offline_services":"up"}\n')
        def __init__(self, *args, **kwargs):
            pass
        def poll(self):
            return 0
    class Heartbeat:
        def __init__(self, stream):
            pass
        def check(self):
            if last_event == "eof":
                raise RuntimeError("Controller heartbeat was lost")
    monkeypatch.setattr(small, "ControllerHeartbeat", Heartbeat)
    monkeypatch.setattr(small.subprocess, "Popen", Process)
    monkeypatch.setattr(small, "process_identity", lambda pid: dict(pid=pid, start_ticks="1", argv_sha256="a" * 64))
    monkeypatch.setattr(small, "child_session", lambda pid: dict(pid=pid, pgid=pid, sid=pid, start_ticks="1"))
    monkeypatch.setattr(small, "stop_session", lambda *args: dict(active_members_after_cleanup=0, kill_escalated=False))
    monkeypatch.setattr(preserved, "stop_service", lambda *args: cleaned.append(True))
    original = small.phase_result
    def result(*args):
        value = original(*args)
        if last_event == "cancel_after_result":
            signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)
        return value
    monkeypatch.setattr(small, "phase_result", result)
    def execute():
        return small.node_execute(current_config(), "runtime/real/config/test.json", tmp_path,
                                  "node-start-offline", None, "a" * 32, preserved=chosen)
    if last_event == "success":
        assert execute()["status"] == "completed"
    else:
        with pytest.raises((RuntimeError, InterruptedError)):
            execute()
    done = json.loads((small.attempt_path(tmp_path, "a" * 32) / "done.json").read_bytes())
    assert done["phase_complete"] is (last_event == "success")
    assert bool(cleaned) is (last_event != "success")
