"""Owned POSIX-session recovery; actual Linux tests use synthetic temporary children only."""
import io
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from snow_statistics import real_offline_small as small
from snow_statistics.io import digest, write_json
from snow_statistics.publication import canonical

ATTEMPT = "d" * 32
SCOPE = dict(pid=1234, pgid=1234, sid=1234, start_ticks="100")
CONFIG = json.loads((Path(__file__).resolve().parents[1] / "deploy/real-run.example.json").read_bytes())


def registered(tmp_path):
    folder = small.attempt_path(tmp_path, ATTEMPT)
    worker = dict(config_sha256=digest(canonical(CONFIG)), phase="land", run_id=None,
                  process=dict(pid=5678, start_ticks="90", argv_sha256="a" * 64))
    child = dict(schema_version=1, attempt=ATTEMPT, worker_sha256=digest(canonical(worker)),
                 command_sha256="b" * 64, session=SCOPE.copy())
    write_json(folder / "worker.json", worker)
    write_json(folder / "child.json", child)
    return folder, worker, child


def fake_proc(monkeypatch, values):
    monkeypatch.setattr(small.signal, "SIGKILL", 9, raising=False)
    monkeypatch.setattr(small, "session_stat", lambda pid: values.get(pid))
    monkeypatch.setattr(small, "Path", lambda path: SimpleNamespace(
        iterdir=lambda: [SimpleNamespace(name=str(pid)) for pid in values]))


def test_members_include_orphan_after_leader_is_gone_and_exclude_other_session(monkeypatch):
    values = {1235: dict(pid=1235, pgid=1234, sid=1234, start_ticks="101", state="S"),
              1240: dict(pid=1240, pgid=1240, sid=1240, start_ticks="99", state="S")}
    fake_proc(monkeypatch, values)
    assert small.session_members(SCOPE) == [1235]


@pytest.mark.parametrize("change", [dict(start_ticks="999"), dict(pgid=9000), dict(sid=9000)])
def test_reused_leader_identity_is_refused_before_any_signal(monkeypatch, change):
    fake_proc(monkeypatch, {1234: dict(SCOPE, state="S") | change})
    monkeypatch.setattr(small.os, "killpg", lambda *_: pytest.fail("No signal on identity mismatch"), raising=False)
    with pytest.raises(ValueError, match="leader identity"):
        small.stop_session(SCOPE)


@pytest.mark.parametrize("change", [dict(sid=7777), dict(start_ticks="99")])
def test_foreign_group_member_is_refused(monkeypatch, change):
    fake_proc(monkeypatch, {1235: dict(pid=1235, pgid=1234, sid=1234, start_ticks="101", state="S") | change})
    with pytest.raises(ValueError, match="Unexpected member"):
        small.session_members(SCOPE)


def test_zombie_leader_is_registered_before_poll_but_not_active(monkeypatch):
    fake_proc(monkeypatch, {1234: dict(SCOPE, state="Z")})
    assert small.child_session(1234) == SCOPE
    assert small.session_members(SCOPE) == []


def test_term_ignoring_descendant_requires_kill_even_if_leader_wait_succeeds(monkeypatch):
    sent, clock = [], [0]
    monkeypatch.setattr(small.signal, "SIGKILL", 9, raising=False)
    monkeypatch.setattr(small, "session_members", lambda _: [] if sent and sent[-1][1] == 9 else [1235])
    monkeypatch.setattr(small.os, "killpg", lambda pid, sig: sent.append((pid, sig)), raising=False)
    monkeypatch.setattr(small.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(small.time, "sleep", lambda _: clock.__setitem__(0, clock[0] + 1))
    process = SimpleNamespace(wait=lambda **_: 9, poll=lambda: 9)
    result = small.stop_session(SCOPE, process)
    assert sent == [(1234, signal.SIGTERM), (1234, 9)]
    assert result == dict(active_members_after_cleanup=0, kill_escalated=True)


def test_dead_worker_cancellation_recovers_registered_group_and_is_idempotent(tmp_path, monkeypatch):
    folder, _, _ = registered(tmp_path)
    actions = []
    monkeypatch.setattr(small, "process_identity", lambda _: None)
    def stop(scope, process=None):
        assert scope == SCOPE and process is None
        actions.append("session")
        return dict(active_members_after_cleanup=0, kill_escalated=False)
    monkeypatch.setattr(small, "stop_session", stop)
    monkeypatch.setattr(small, "driver_cleanup", lambda *_: actions.append("driver"))
    assert small.node_cancel(tmp_path, CONFIG, ATTEMPT)["status"] == "cancelled"
    assert actions == ["session", "driver"]
    done = json.loads((folder / "done.json").read_bytes())
    assert done["child_group_stopped"] and done["driver_stopped"] and not done["phase_complete"]
    monkeypatch.setattr(small, "driver_cleanup", lambda *_: pytest.fail("Do not stop a later driver's CID"))
    monkeypatch.setattr(small, "stop_session", lambda *_: pytest.fail("Do not signal a reused historical PID"))
    assert small.node_cancel(tmp_path, CONFIG, ATTEMPT)["already_cleaned"]


def test_worker_exits_between_identity_read_and_signal_still_cleans_child(tmp_path, monkeypatch):
    folder, worker, _ = registered(tmp_path)
    identities, actions = iter([worker["process"], None, None]), []
    monkeypatch.setattr(small, "process_identity", lambda _: next(identities))
    monkeypatch.setattr(small.os, "kill", lambda *_: (_ for _ in ()).throw(ProcessLookupError("synthetic exited worker")))
    def stop(scope, process=None):
        assert scope == SCOPE
        actions.append("child-session")
        return dict(active_members_after_cleanup=0, kill_escalated=False)
    monkeypatch.setattr(small, "stop_session", stop)
    monkeypatch.setattr(small, "driver_cleanup", lambda *_: actions.append("driver"))
    assert small.node_cancel(tmp_path, CONFIG, ATTEMPT)["status"] == "cancelled"
    assert actions == ["child-session", "driver"]
    assert json.loads((folder / "done.json").read_bytes())["child_group_stopped"]


@pytest.mark.parametrize("field,value", [("worker_sha256", "c" * 64), ("attempt", "e" * 32),
                                         ("session", dict(SCOPE, sid=9000))])
def test_cancel_rejects_tampered_child_scope_without_signalling(tmp_path, monkeypatch, field, value):
    folder, _, child = registered(tmp_path)
    child[field] = value
    write_json(folder / "child.json", child)
    monkeypatch.setattr(small.os, "kill", lambda *_: pytest.fail("No parent signal"))
    monkeypatch.setattr(small, "stop_session", lambda *_: pytest.fail("No group signal"))
    with pytest.raises(ValueError):
        small.node_cancel(tmp_path, CONFIG, ATTEMPT)


def test_legacy_dead_worker_without_child_scope_is_unresolved(tmp_path, monkeypatch):
    folder, _, _ = registered(tmp_path)
    (folder / "child.json").unlink()
    monkeypatch.setattr(small, "process_identity", lambda _: None)
    monkeypatch.setattr(small, "stop_session", lambda *_: pytest.fail("Never guess legacy PGID"))
    actions = []
    monkeypatch.setattr(small, "driver_cleanup", lambda *_: actions.append("driver"))
    with pytest.raises(RuntimeError, match="Legacy"):
        small.node_cancel(tmp_path, CONFIG, ATTEMPT)
    assert actions == ["driver"] and not (folder / "done.json").exists()


def test_completion_hash_mismatch_cannot_certify_old_cleanup(tmp_path, monkeypatch):
    folder, worker, child = registered(tmp_path)
    write_json(folder / "done.json", dict(schema_version=1, attempt=ATTEMPT, worker_sha256=digest(canonical(worker)),
               child_sha256="0" * 64, launch_attempted=True, phase_complete=False, child_group_stopped=True,
               driver_stopped=True, session_cleanup=dict(active_members_after_cleanup=0, kill_escalated=False)))
    monkeypatch.setattr(small.os, "kill", lambda *_: pytest.fail("No signal for tampered completion"))
    monkeypatch.setattr(small, "driver_cleanup", lambda *_: pytest.fail("No driver action"))
    with pytest.raises(ValueError, match="Completion receipt"):
        small.node_cancel(tmp_path, CONFIG, ATTEMPT)
    assert small.checked_child(folder, worker, ATTEMPT) == child


def test_cancellation_after_admission_but_before_launch_never_starts_child(tmp_path, monkeypatch):
    class Heartbeat:
        def __init__(self, stream):
            signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)
    monkeypatch.setattr(small, "ControllerHeartbeat", Heartbeat)
    monkeypatch.setattr(small, "process_identity", lambda pid: dict(pid=pid, start_ticks="1", argv_sha256="a" * 64))
    monkeypatch.setattr(small.subprocess, "Popen", lambda *_a, **_k: pytest.fail("No late child launch"))
    monkeypatch.setattr(small, "driver_cleanup", lambda *_: None)
    with pytest.raises(InterruptedError, match="cancelled"):
        small.node_execute(CONFIG, "runtime/real/config/test.json", tmp_path, "land", None, ATTEMPT)
    folder = small.attempt_path(tmp_path, ATTEMPT)
    done = json.loads((folder / "done.json").read_bytes())
    assert not done["launch_attempted"] and done["child_group_stopped"]
    assert not (folder / "child.json").exists()
    assert small.node_cancel(tmp_path, CONFIG, ATTEMPT)["already_cleaned"]


def test_cancelled_signal_during_popen_is_deferred_until_child_registration(tmp_path, monkeypatch):
    scope, cleaned = SCOPE.copy(), []
    class Process:
        pid, stdout = 1234, io.BytesIO(b"")
        def __init__(self, *_a, **_k):
            # Invoke the installed handler, without sending a system signal.
            signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)
        def poll(self):
            pytest.fail("Deferred cancellation is checked before poll")
    monkeypatch.setattr(small.subprocess, "Popen", Process)
    monkeypatch.setattr(small, "process_identity", lambda pid: dict(pid=pid, start_ticks="1", argv_sha256="a" * 64))
    monkeypatch.setattr(small, "child_session", lambda _: scope)
    monkeypatch.setattr(small, "driver_cleanup", lambda *_: cleaned.append("driver"))
    def stop(identity, process=None):
        child = json.loads((small.attempt_path(tmp_path, ATTEMPT) / "child.json").read_bytes())
        assert child["session"] == identity == scope
        cleaned.append("session")
        return dict(active_members_after_cleanup=0, kill_escalated=False)
    monkeypatch.setattr(small, "stop_session", stop)
    with pytest.raises(InterruptedError, match="cancelled"):
        small.node_execute(CONFIG, "runtime/real/config/test.json", tmp_path, "land", None, ATTEMPT,
                           SimpleNamespace(fileno=lambda: 123))
    assert cleaned == ["session", "driver"]
    done = json.loads((small.attempt_path(tmp_path, ATTEMPT) / "done.json").read_bytes())
    assert done["launch_attempted"] and done["child_group_stopped"] and not done["phase_complete"]


def wait_file(path, seconds=10):
    until = time.monotonic() + seconds
    while not path.exists():
        if time.monotonic() >= until:
            raise TimeoutError("Synthetic child marker deadline")
        time.sleep(0.02)


@pytest.mark.skipif(os.name != "posix", reason="Actual Linux orphan process-group ownership and readback")
@pytest.mark.parametrize("ignore_term", [False, True])
def test_actual_exited_leader_orphan_cleanup_preserves_bystander(tmp_path, ignore_term):
    marker, release = tmp_path / "child.pid", tmp_path / "release"
    grandchild = ("import os,pathlib,time,signal; "
                  + ("signal.signal(signal.SIGTERM,signal.SIG_IGN); " if ignore_term else "")
                  + f"pathlib.Path({str(marker)!r}).write_text(str(os.getpid())); time.sleep(40)")
    body = (f"import subprocess,sys,pathlib,time,os; subprocess.Popen([sys.executable,'-c',{grandchild!r}]); "
            f"release=pathlib.Path({str(release)!r}); until=time.monotonic()+20\n"
            "while not release.exists() and time.monotonic()<until: time.sleep(.02)\n"
            "os._exit(9)\n")
    leader = subprocess.Popen([sys.executable, "-c", body], start_new_session=True)
    scope = small.child_session(leader.pid)
    bystander = subprocess.Popen([sys.executable, "-c", "import time;time.sleep(40)"], start_new_session=True)
    bystander_scope = small.child_session(bystander.pid)
    try:
        wait_file(marker)
        child = int(marker.read_text())
        release.touch()
        assert leader.wait(timeout=5) == 9
        assert small.process_identity(child) is not None
        result = small.stop_session(scope, leader)
        assert result["kill_escalated"] is ignore_term
        assert small.process_identity(child) is None and small.session_members(scope) == []
        assert bystander.poll() is None
    finally:
        small.stop_session(scope, leader)
        small.stop_session(bystander_scope, bystander)


@pytest.mark.skipif(os.name != "posix", reason="Actual Linux node finalizer after its child leader exits")
@pytest.mark.parametrize("exit_code", [0, 9])
def test_actual_node_stops_orphan_holding_stdout_without_masking_exit(tmp_path, exit_code):
    (tmp_path / "tools").mkdir()
    child_code = "import os,pathlib,time; pathlib.Path('grandchild.pid').write_text(str(os.getpid())); time.sleep(40)"
    (tmp_path / "tools/real_lab.py").write_text(
        "import subprocess,sys,pathlib,time\n"
        f"subprocess.Popen([sys.executable,'-c',{child_code!r}])\n"
        "until=time.monotonic()+10\n"
        "while not pathlib.Path('grandchild.pid').exists() and time.monotonic()<until: time.sleep(.02)\n"
        "print('{\"offline_services\":\"up\"}',flush=True)\n"
        f"sys.exit({exit_code})\n")
    code = ("import json,sys,pathlib\nfrom snow_statistics import real_offline_small as s\n"
            "s.driver_cleanup=lambda *a:None\n"
            "print(json.dumps(s.node_execute(json.loads(sys.argv[2]),'runtime/real/config/test.json',"
            "pathlib.Path(sys.argv[1]),'node-start-offline',None,'d'*32)))\n")
    parent = subprocess.Popen([sys.executable, "-c", code, str(tmp_path), json.dumps(CONFIG)],
                              stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
    parent_scope = small.child_session(parent.pid)
    try:
        parent.stdin.write(b"ping\n")
        parent.stdin.flush()
        parent.wait(timeout=15)
        errors = parent.stderr.read()
        assert (parent.returncode == 0) is (exit_code == 0), errors.decode()
        child = int((tmp_path / "grandchild.pid").read_text())
        assert small.process_identity(child) is None
        directory = small.attempt_path(tmp_path, ATTEMPT)
        done = json.loads((directory / "done.json").read_bytes())
        assert done["child_group_stopped"] and done["driver_stopped"]
        assert done["phase_complete"] is (exit_code == 0)
    finally:
        small.stop_session(parent_scope, parent)
        child_file = small.attempt_path(tmp_path, ATTEMPT) / "child.json"
        if child_file.exists():
            small.stop_session(json.loads(child_file.read_bytes())["session"])
        for stream in (parent.stdin, parent.stdout, parent.stderr):
            stream.close()


@pytest.mark.skipif(os.name != "posix", reason="Actual Linux recovery of a dead worker's durable child scope")
def test_actual_node_cancel_recovers_after_worker_exits_without_finally(tmp_path, monkeypatch):
    code = ("import json,sys,pathlib,subprocess,os\n"
            "from snow_statistics import real_offline_small as s\n"
            "root=pathlib.Path(sys.argv[1]); config=json.loads(sys.argv[2]); attempt='d'*32\n"
            "folder=s.attempt_path(root,attempt)\n"
            "worker=dict(config_sha256=s.digest(s.canonical(config)),phase='land',run_id=None,process=s.process_identity(os.getpid()))\n"
            "s.write_json(folder/'worker.json',worker)\n"
            "command=[sys.executable,'-c','import time;time.sleep(40)']\n"
            "child=subprocess.Popen(command,start_new_session=True)\n"
            "identity=s.child_session(child.pid)\n"
            "s.write_json(folder/'child.json',dict(schema_version=1,attempt=attempt,worker_sha256=s.digest(s.canonical(worker)),"
            "command_sha256=s.digest(s.canonical(command)),session=identity))\n"
            "os._exit(11)\n")
    parent = subprocess.Popen([sys.executable, "-c", code, str(tmp_path), json.dumps(CONFIG)], start_new_session=True)
    parent_scope = small.child_session(parent.pid)
    monkeypatch.setattr(small, "driver_cleanup", lambda *_: None)
    try:
        assert parent.wait(timeout=10) == 11
        folder = small.attempt_path(tmp_path, ATTEMPT)
        identity = json.loads((folder / "child.json").read_bytes())["session"]
        assert small.session_members(identity)
        assert small.node_cancel(tmp_path, CONFIG, ATTEMPT)["status"] == "cancelled"
        assert small.session_members(identity) == []
        assert json.loads((folder / "done.json").read_bytes())["child_group_stopped"]
    finally:
        small.stop_session(parent_scope, parent)
        child = small.attempt_path(tmp_path, ATTEMPT) / "child.json"
        if child.exists():
            small.stop_session(json.loads(child.read_bytes())["session"])
