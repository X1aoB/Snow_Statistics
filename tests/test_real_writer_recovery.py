"""Synthetic adapter responses only; these are not Flink engine receipts."""
import copy
from datetime import timedelta
from types import SimpleNamespace

import pytest
from test_real_quiescent import COLLECTOR, NOW, candidate

from snow_statistics.io import digest, write_json
from snow_statistics.real_quiescent import RecoveryLedger, WindowGuard, expected_parameters
from snow_statistics.real_writer_recovery import ActualRecoveryProbe, WriterRecovery, normalize_checkpoint


class Probe:
    def __init__(self, registry, clock):
        self.registry, self.clock = registry, clock
        self.states = {"a" * 32: "RUNNING"}
        self.new_job = "b" * 32
        self.calls = []
        self.corrupt, self.wrong_restore, self.missing = False, False, False
        self.submits = 0

    def jobs(self):
        return [dict(jid=job, state=state) for job, state in self.states.items()]

    def job(self, job):
        return {"jid": job, "state": self.states[job]}

    def verify_jar(self, registration):
        self.calls.append("jar")

    def latest(self, job, registration, now):
        if self.missing:
            raise ValueError("no checkpoint")
        self.calls.append("latest")
        return dict(job_id=job, id=10, external_path=f"file:///checkpoints/{job}/chk-10",
                    trigger_timestamp=int((now - timedelta(milliseconds=5)).timestamp() * 1000),
                    latest_ack_timestamp=int((now - timedelta(milliseconds=2)).timestamp() * 1000))

    def cancel(self, job):
        # The pause intent must already close even per-row producer admission.
        with pytest.raises(ValueError, match="synchronization remains closed"):
            self.registry.sync_ready(now=self.clock())
        self.calls.append("cancel")
        self.states[job] = "CANCELED"

    def tree(self, jobs, checkpoint):
        self.calls.append("tree")
        return [dict(path=f"{job}/chk-10/_metadata", bytes=3,
                     sha256=digest(b"corrupted" if self.corrupt else b"synthetic checkpoint")) for job in sorted(jobs)]

    def submit(self, checkpoint, registration, sequence):
        self.calls.append("submit")
        self.submits += 1
        self.states[self.new_job] = "RUNNING"
        return dict(job_id=self.new_job, environment_sha256="a" * 64)

    def restored(self, job, checkpoint):
        registration = self.registry.ready(now=self.clock())
        return dict(job_id=job, readback=dict(job_id=job, state="RUNNING", jar_sha256=registration["jar_sha256"],
            parameters=expected_parameters(self.registry.epoch.read(), registration["initial"]["kafka"])),
            restored=dict(id=checkpoint["id"], external_path="file:///wrong" if self.wrong_restore else checkpoint["external_path"],
                          restore_timestamp=int(self.clock().timestamp() * 1000), is_savepoint=True))


def setup(tmp_path):
    registry, docker, _ = candidate(tmp_path)
    def clock():
        return NOW + timedelta(seconds=1)
    writer = SimpleNamespace(registry=registry, epoch=registry.epoch, identity=lambda: COLLECTOR,
                             manifest=registry.epoch.read(), directory=registry.epoch.directory / "writer")
    probe = Probe(registry, clock)
    recovery = WriterRecovery(writer, probe=probe, clock=clock, sleep=lambda _: None)
    return recovery, registry, docker, probe, clock


def restart(docker):
    for item in docker.c.values():
        item["running"] = True


def test_pause_resume_preserves_initial_receipt_and_appends_bound_new_job(tmp_path):
    recovery, registry, docker, probe, clock = setup(tmp_path)
    original = registry.job_path.read_bytes()
    paused = recovery.pause()
    assert paused["action"] == "paused"
    assert not any(v["running"] for v in docker.c.values()) and len(docker.v) == 5
    assert paused["expires_at"] == registry.epoch.read()["expires_at"]
    assert probe.calls.index("latest") < probe.calls.index("cancel") < probe.calls.index("tree")
    with pytest.raises(ValueError, match="synchronization remains closed"):
        registry.sync_ready(now=clock())
    restart(docker)
    result = recovery.resume()
    assert result["action"] == "resumed"
    assert registry.current_job(now=clock()) == "b" * 32
    assert registry.sync_ready(now=clock())["collector"] == COLLECTOR
    assert registry.job_path.read_bytes() == original
    assert [v["action"] for v in registry.recovery(now=clock())] == [
        "pause_requested", "paused", "resume_requested", "resume_acknowledged", "resumed"]
    # Another ordinary same-epoch stop is a new append, retaining prior jobs.
    second = recovery.pause()
    assert second["payload"]["job_id"] == "b" * 32
    restart(docker)
    probe.new_job = "c" * 32
    recovery.resume()
    assert registry.current_job(now=clock()) == "c" * 32
    assert len(registry.recovery(now=clock())) == 10


def test_busy_sync_lease_does_not_stop_any_engine_or_write_pause_intent(tmp_path):
    recovery, registry, docker, _, _ = setup(tmp_path)
    with registry.operation_lock(), pytest.raises((OSError, BlockingIOError)):
        recovery.pause()
    assert all(v["running"] for v in docker.c.values())
    assert not (registry.epoch.directory / "writer-recovery").exists()


@pytest.mark.parametrize("case", ["missing_checkpoint", "unknown_job", "old_job_failed", "bad_volume"])
def test_pause_requires_actual_checkpoint_current_job_and_storage(tmp_path, case):
    recovery, registry, docker, probe, clock = setup(tmp_path)
    if case == "missing_checkpoint":
        probe.missing = True
    elif case == "unknown_job":
        probe.states["e" * 32] = "RUNNING"
    elif case == "old_job_failed":
        probe.states["a" * 32] = "FAILED"
    else:
        next(iter(docker.v.values()))["CreatedAt"] = "replaced"
    with pytest.raises(ValueError):
        recovery.pause()
    assert not any(v["running"] for v in docker.c.values())
    assert probe.submits == 0
    if case != "bad_volume":
        with pytest.raises(ValueError, match="synchronization remains closed"):
            registry.sync_ready(now=clock())


@pytest.mark.parametrize("case", ["corrupt", "new_job_present", "reused_job_id", "wrong_restored", "expired", "collector_changed"])
def test_resume_rejects_changed_state_or_binding_and_stops_only_epoch(tmp_path, case):
    recovery, registry, docker, probe, clock = setup(tmp_path)
    recovery.pause()
    restart(docker)
    if case == "corrupt":
        probe.corrupt = True
    elif case == "new_job_present":
        probe.states["d" * 32] = "RUNNING"
    elif case == "reused_job_id":
        probe.new_job = "a" * 32
    elif case == "wrong_restored":
        probe.wrong_restore = True
    elif case == "expired":
        recovery.clock = lambda: NOW + timedelta(days=8)
    else:
        recovery.writer.identity = lambda: COLLECTOR | {"generation": "00000000-0000-0000-0000-000000000099"}
    with pytest.raises(ValueError):
        recovery.resume()
    assert not any(v["running"] for v in docker.c.values()) and len(docker.v) == 5
    with pytest.raises(ValueError):
        registry.sync_ready(now=clock())


def test_acknowledgement_interruption_cannot_silently_adopt_running_job(tmp_path, monkeypatch):
    recovery, registry, docker, probe, clock = setup(tmp_path)
    recovery.pause()
    restart(docker)
    monkeypatch.setattr(probe, "restored", lambda *a: (_ for _ in ()).throw(ConnectionError("REST interrupted")))
    with pytest.raises(ConnectionError):
        recovery.resume()
    assert registry.recovery(now=clock())[-1]["action"] == "resume_acknowledged"
    restart(docker)
    with pytest.raises(ValueError, match="interrupted submissions"):
        recovery.resume()
    assert probe.submits == 1


@pytest.mark.parametrize("case", ["remove", "extra", "hash", "deadline", "head", "partial"])
def test_append_only_ledger_corruption_and_partial_append_fail_closed(tmp_path, case):
    recovery, registry, _, _, clock = setup(tmp_path)
    recovery.pause()
    folder = recovery.ledger.directory
    if case == "remove":
        (folder / "000002.json").unlink()
    elif case == "extra":
        write_json(folder / "000003.json", {})
    elif case == "partial":
        (folder / "head.json.tmp").write_bytes(b"{")
    else:
        import json
        path = folder / ("head.json" if case == "head" else "000002.json")
        data = json.loads(path.read_bytes())
        if case == "head":
            data["sha256"] = "0" * 64
        elif case == "deadline":
            data["expires_at"] = (clock() + timedelta(days=90)).isoformat()
        else:
            data["payload"]["files"][0]["sha256"] = "0" * 64
        write_json(path, data)
    with pytest.raises(ValueError):
        registry.sync_ready(now=clock())


def test_pause_intent_clears_per_row_window_guard_without_moving_cursor(tmp_path):
    recovery, registry, _, _, clock = setup(tmp_path)
    source = tmp_path / "source.json"
    write_json(source, COLLECTOR)
    guard = WindowGuard(registry, source, clock=clock)
    row = {"source": "real", "accepted_at": NOW.isoformat()}
    guard(row)
    RecoveryLedger(registry).append("pause_requested", {"job_id": "a" * 32}, now=clock())
    with pytest.raises(ValueError, match="synchronization remains closed"):
        guard(row)


def test_complete_pause_can_get_stopped_admission_but_pending_restore_cannot(tmp_path):
    from test_real_quiescent import snapshot

    from snow_statistics.publication import canonical
    from snow_statistics.real_quiescent import StoppedStorage
    recovery, registry, _, _, clock = setup(tmp_path)
    recovery.pause()
    assert StoppedStorage(registry, snapshot(registry)).check(now=clock())["verification"] == "stopped_storage_unexpired"
    checkpoint = registry.recovery(now=clock())[-1]["payload"]["checkpoint"]
    RecoveryLedger(registry).append("resume_requested", dict(job_id="a" * 32,
        checkpoint_sha256=digest(canonical(checkpoint)), restore_mode="NO_CLAIM"), now=clock())
    with pytest.raises(ValueError, match="Interrupted writer recovery"):
        StoppedStorage(registry, snapshot(registry)).check(now=clock())


def test_doris_guard_checks_pending_recovery_before_any_physical_or_sql_access(tmp_path, monkeypatch):
    from snow_statistics.real_quiescent import validate_doris_write
    recovery, registry, _, _, clock = setup(tmp_path)
    RecoveryLedger(registry).append("pause_requested", {"job_id": "a" * 32}, now=clock())
    def forbidden(*a, **kwargs):
        raise AssertionError("must stop before accessing physical or SQL state")
    monkeypatch.setattr("snow_statistics.real_quiescent.storage", forbidden)
    with pytest.raises(ValueError, match="Interrupted writer recovery"):
        validate_doris_write({}, registry, COLLECTOR, now=clock())


@pytest.mark.parametrize("path", ["file:///elsewhere/x", "file://remote/checkpoints/x", "file:///checkpoints/../secret",
                                  "file:///checkpoints/" + "a" * 32 + "/chk-1?x=1"])
def test_checkpoint_url_cannot_escape_exact_scope(path):
    with pytest.raises(ValueError):
        normalize_checkpoint(path, "a" * 32, 1)


def test_actual_probe_requires_retained_complete_checkpoint_and_normalizes_file_uri(tmp_path):
    recovery, registry, _, _, clock = setup(tmp_path)
    cp = dict(status="COMPLETED", discarded=False, is_savepoint=False, checkpoint_type="CHECKPOINT",
              num_subtasks=7, num_acknowledged_subtasks=7, id=1, external_path="file:/checkpoints/" + "a" * 32 + "/chk-1",
              trigger_timestamp=int(NOW.timestamp() * 1000), latest_ack_timestamp=int(clock().timestamp() * 1000))
    cfg = dict(externalization={"enabled": True, "delete_on_cancellation": False})
    recovery.writer.flink = lambda path: cfg if path.endswith("/config") else {"latest": {"completed": cp}}
    probe = ActualRecoveryProbe(recovery.writer)
    result = probe.latest("a" * 32, registry.ready(now=clock()), clock())
    assert result["external_path"].startswith("file:///")
    cp["num_acknowledged_subtasks"] = 6
    with pytest.raises(ValueError, match="fully completed"):
        probe.latest("a" * 32, registry.ready(now=clock()), clock())
    cp["num_acknowledged_subtasks"] = 7
    cfg["externalization"]["delete_on_cancellation"] = True
    with pytest.raises(ValueError, match="retain state"):
        probe.latest("a" * 32, registry.ready(now=clock()), clock())


@pytest.mark.parametrize("bad", ["link", "foreign", "hardlink", "corrupt_output", "changed"])
def test_actual_hash_inventory_rejects_unregistered_or_unstable_resources(tmp_path, bad):
    recovery, registry, _, _, clock = setup(tmp_path)
    cp = dict(job_id="a" * 32, id=1)
    rel = "a" * 32 + "/chk-1/_metadata"
    fields = ["f", "1", "3", rel]
    if bad == "link":
        fields[0] = "l"
    elif bad == "foreign":
        fields[3] = "b" * 32 + "/chk-1/_metadata"
    elif bad == "hardlink":
        fields[1] = "2"
    raw = ("\0".join(fields) + "\0").encode()
    calls = []
    def command(args, **kwargs):
        assert all("\0" not in arg for arg in args)
        calls.append(args)
        if "find" in args:
            return raw + (b"extra" if bad == "changed" and len(calls) > 1 else b"")
        return ("a" * 64 + "  " + ("/other" if bad == "corrupt_output" else "/checkpoints/" + rel) + "\n").encode()
    recovery.writer.epoch.docker.command = command
    with pytest.raises(ValueError):
        ActualRecoveryProbe(recovery.writer).tree({"a" * 32}, cp)


def test_actual_restore_command_disallows_nonrestored_state_and_freezes_jar(tmp_path):
    recovery, registry, _, _, clock = setup(tmp_path)
    writer = recovery.writer
    writer.directory.mkdir()
    writer.host = "192.168.100.103"
    writer.account = lambda: {"user": "sr_candidate_one", "password": "a" * 43}
    calls = []
    def command(args, **kwargs):
        calls.append(args)
        if "sha256sum" in args:
            return (writer.manifest["jar"]["sha256"] + "  /opt/flink/usrlib/snow-realtime.jar\n").encode()
        return ("Job has been submitted with JobID " + "b" * 32).encode()
    writer.epoch.docker.command = command
    probe = ActualRecoveryProbe(writer)
    cp = {"external_path": "file:///checkpoints/" + "a" * 32 + "/chk-1"}
    probe.submit(cp, registry.ready(now=clock()), 3)
    actual = calls[-1]
    assert actual[actual.index("--claimMode") + 1] == "no_claim"
    assert actual[actual.index("--fromSavepoint") + 1] == cp["external_path"]
    assert "-n" not in actual and "--allowNonRestoredState" not in actual
    assert not any("SNOW_REAL_EPOCH_" in line for line in (writer.directory / "recovery-000003.env").read_text().splitlines())
    with pytest.raises(ValueError, match="cannot be reused"):
        probe.submit(cp, registry.ready(now=clock()), 3)


def test_actual_running_job_without_this_submission_or_wrong_restore_is_rejected(tmp_path):
    recovery, registry, _, _, clock = setup(tmp_path)
    probe = ActualRecoveryProbe(recovery.writer)
    cp = dict(job_id="a" * 32, id=1, external_path="file:///checkpoints/" + "a" * 32 + "/chk-1")
    with pytest.raises(ValueError, match="No actual recovery"):
        probe.restored("b" * 32, cp)
    probe.submitted = dict(job_id="b" * 32, jar_sha256="a" * 64, parameters={})
    value = dict(counts={"restored": 1}, latest={"restored": dict(id=2, is_savepoint=False,
                 external_path="file:/checkpoints/" + "a" * 32 + "/chk-2", restore_timestamp=int(clock().timestamp() * 1000))})
    recovery.writer.flink = lambda path: copy.deepcopy(value) if path.endswith("/checkpoints") else dict(
        jid="b" * 32, name="Snow Statistics real candidate_one", state="RUNNING")
    with pytest.raises(ValueError, match="differs"):
        probe.restored("b" * 32, cp)
    value["latest"]["restored"].update(id=1, is_savepoint=True, external_path=cp["external_path"])
    assert probe.restored("b" * 32, cp)["restored"]["is_savepoint"] is True
