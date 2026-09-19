"""Synthetic aggregate pairs and fake backends only; no SSH, Docker or real records."""
import copy
import io
import json
import shutil
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_real_publication import packages

from snow_statistics import real_lake_authority as authority
from snow_statistics import real_lake_dispatch as dispatch
from snow_statistics.io import digest, write_json
from snow_statistics.lifecycle import RealLifecycle, timestamp
from snow_statistics.publication import canonical
from snow_statistics.real_publication import release_real
from snow_statistics.real_transfer import export

ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 9, 19, 9, tzinfo=UTC)
RUN = "aggregate-01"
ATTEMPT = "lake-01"


class Clock(datetime):
    current = NOW

    @classmethod
    def now(cls, tz=None):
        return cls.current


@pytest.fixture
def prepared(tmp_path, monkeypatch):
    Clock.current = NOW
    monkeypatch.setattr(authority, "datetime", Clock)
    monkeypatch.setattr(dispatch, "datetime", Clock)
    value = json.loads((ROOT / "deploy/real-run.example.json").read_bytes())
    value.update(lane="fixture-lake", input_origin="synthetic fixtures", tunnel=None)
    roots = {name: tmp_path / name for name in ("analysis", "operator", "control")}
    daily, behavior = packages()
    prefix = "hdfs://" + value["nodes"]["snow-control"] + ":9000/snow/"
    for package in (daily, behavior):
        package["manifest"]["input"] = prefix + "ods/real/kafka/fixture-lake/snapshots/" + "a" * 64 + "/_snapshot.json"
        package["manifest"]["input_snapshot"]["offsets"] = {"snow.real.fixture_lake.events.v1:0": 2}
    release = release_real(daily, behavior, roots["analysis"] / "runtime/real/publication", RUN, now=NOW)
    export(roots["analysis"], value, RUN, now=NOW)
    remote = authority.manager(roots["analysis"], value)
    identity = daily["manifest"]["input_snapshot"]["collector"]
    remote.initialize(prefix + "warehouse/real/fixture-lake", prefix + "auxiliary/real/fixture-lake",
                      prefix + "ods/real/kafka/fixture-lake", identity["instance_id"], identity["generation"])
    calls = []

    def actual_cleanup(config, root, phase, run_id):
        assert Path(root) == roots["analysis"] and phase == "cleanup" and run_id is None
        owner, registry = remote._read()
        calls.append(dict(phase=phase, artifacts=copy.deepcopy(registry["artifacts"])))
        result = dict(schema_version=1, source="real", owner={key: owner[key] for key in ("instance_id", "generation")},
                      checked_at=Clock.current.isoformat(), registry_sha256=digest(canonical(registry)),
                      next_expiry=min(entry["expires_at"] for entry in registry["artifacts"].values()))
        write_json(remote.directory / "last-cleanup.json", result)
        return result

    monkeypatch.setattr(authority, "lifecycle_phase", actual_cleanup)
    for root in roots.values():
        for name in (*authority.ENGINE_FILES, "lab/locks/images.env", "lab/locks/jars.json"):
            target = root / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, target)
    descriptor = authority.prepare(roots["analysis"], value, RUN, ATTEMPT, now=NOW)
    return value, roots, descriptor, remote, calls, release


def transfer(prepared, destination):
    value, roots, descriptor, *_ = prepared
    source = authority.location(roots["analysis"], value, RUN, ATTEMPT) / "data/package.json"
    local = authority.reserve(roots[destination], value, RUN, ATTEMPT, descriptor, now=NOW)
    shutil.copyfile(source, local.path("package.json.tmp"))
    authority.accept(roots[destination], value, RUN, ATTEMPT, now=NOW)
    return local


def receipt(bundle, application="application_123_0001"):
    return dict(schema_version=1, source="real", run_id=ATTEMPT, warehouse=bundle["warehouse"],
                input_sha256=digest(canonical(bundle)), expires_at=bundle["expires_at"], engine="Spark 3.5.7",
                master="yarn", application_id=application, hive_registration=False, column_lineage=False,
                tables={name: dict(name="real_lake.analytics." + name, rows=len(rows), exact_readback_equal=True,
                                   original_snapshot=10 if rows else None, current_snapshot=10 if rows else None,
                                   historical_readback_equal=True if rows else None, location=bundle["warehouse"] + "/analytics/" + name,
                                   additive_column="model_revision") for name, rows in bundle["aggregates"].items()})


def test_authority_registers_scope_before_actual_cleanup_and_never_copies_registry(prepared):
    value, roots, descriptor, remote, calls, release = prepared
    assert len(calls) == 1 and descriptor["scope"] in calls[0]["artifacts"]
    assert descriptor["read_until"] == (NOW + timedelta(minutes=15)).isoformat()
    assert timestamp(descriptor["expires_at"]) == timestamp(release["expires_at"])
    for target in ("operator", "control"):
        local = transfer(prepared, target)
        assert timestamp(local._read()["artifacts"]["package.json.tmp"]["expires_at"]) == timestamp(release["expires_at"])
        assert not authority.manager(roots[target], value).registry.exists()
    assert authority.prepare(roots["analysis"], value, RUN, ATTEMPT, now=NOW) == descriptor
    assert len(calls) == 1  # Immutable unexpired preparation is idempotent.
    assert "anonymous_id" not in (authority.location(roots["control"], value, RUN, ATTEMPT) / "data/input.json").read_text()


@pytest.mark.parametrize("change", [
    lambda d: d.update(source="synthetic"),
    lambda d: d.update(collector=d["collector"] | {"generation": "c4184e1c-3cbe-4a59-a631-9f6215699c29"}),
    lambda d: d.update(scope="hdfs://outside:9000/business"),
    lambda d: d.update(read_until=(NOW + timedelta(minutes=16)).isoformat()),
    lambda d: d.update(expires_at=(NOW + timedelta(days=90)).isoformat()),
    lambda d: d.update(package_bytes=authority.MAX_BYTES + 1),
    lambda d: d.update(schema_version=True),
    lambda d: d.update(raw_events=[]),
])
def test_metadata_source_generation_scope_hash_and_lifetime_are_bounded(prepared, change):
    value, roots, descriptor, *_ = prepared
    wrong = copy.deepcopy(descriptor)
    change(wrong)
    with pytest.raises(ValueError):
        authority.reserve(roots["operator"], value, RUN, ATTEMPT, wrong, now=NOW)


def test_partial_copy_is_rejected_remains_registered_and_expires_at_original_time(prepared):
    value, roots, descriptor, _, _, release = prepared
    local = authority.reserve(roots["control"], value, RUN, ATTEMPT, descriptor, now=NOW)
    local.path("package.json.tmp").write_bytes(b'{"release":')
    with pytest.raises(ValueError, match="checksum"):
        authority.accept(roots["control"], value, RUN, ATTEMPT, now=NOW)
    assert local.path("package.json.tmp").exists() and not local.path("input.json").exists()
    authority.cleanup_copies(roots["control"], now=timestamp(release["expires_at"]) + timedelta(seconds=1))
    assert not local.path("package.json.tmp").exists()


def test_unregistered_tampered_or_expired_payload_is_never_read(prepared):
    value, roots, descriptor, *_ = prepared
    local = transfer(prepared, "control")
    with pytest.raises(ValueError, match="deadline"):
        authority.read_attempt(roots["control"], value, RUN, ATTEMPT, now=NOW + timedelta(minutes=15))
    local.path("input.json").write_bytes(b'{}')
    with pytest.raises(ValueError, match="input changed"):
        authority.read_attempt(roots["control"], value, RUN, ATTEMPT, now=NOW)
    write_json(local.root / "unregistered.json", {})
    with pytest.raises(ValueError, match="Unregistered"):
        authority.cleanup_copies(roots["control"], now=NOW)


def test_registry_drift_and_cleanup_failure_prevent_admission(prepared, monkeypatch):
    value, roots, descriptor, remote, *_ = prepared
    registry = json.loads(remote.registry.read_bytes())
    del registry["artifacts"][descriptor["scope"]]
    write_json(remote.registry, registry)
    with pytest.raises(ValueError, match="Authority"):
        authority.prepare(roots["analysis"], value, RUN, ATTEMPT, now=NOW)

    def fail(*_):
        raise RuntimeError("synthetic actual backend cleanup failure")

    monkeypatch.setattr(authority, "lifecycle_phase", fail)
    with pytest.raises(RuntimeError, match="backend cleanup"):
        authority.prepare(roots["analysis"], value, RUN, "new-attempt", now=NOW)
    assert not (authority.location(roots["analysis"], value, RUN, "new-attempt") / "descriptor.json").exists()


def test_execution_and_independent_verification_run_before_authority_confirmation(prepared, monkeypatch):
    value, roots, descriptor, remote, calls, _ = prepared
    local = transfer(prepared, "control")
    runs = []

    def fake_spark(root, directory, descriptor, mode):
        runs.append(mode)
        bundle = json.loads(local.path("input.json").read_bytes())
        output = "receipt.json" if mode == "execute" else "verified-engine.json"
        write_json(local.path(output), receipt(bundle, "application_123_0001" if mode == "execute" else "application_123_0002"))
        return dict(bytes=1, retained_bytes=1, truncated=False)

    monkeypatch.setattr(dispatch, "run_spark", fake_spark)
    # read_attempt's explicit clock argument is absent inside dispatch; its module Clock is frozen above.
    dispatch.execute(roots["control"], value, RUN, ATTEMPT)
    dispatch.execute(roots["control"], value, RUN, ATTEMPT)
    assert runs == ["execute"]
    proof = dispatch.verify(roots["control"], value, RUN, ATTEMPT)
    target = authority.location(roots["analysis"], value, RUN, ATTEMPT) / "data/verify.json"
    shutil.copyfile(local.path("verify.json"), target)
    confirmed = authority.confirm(roots["analysis"], value, RUN, ATTEMPT, proof["evidence_sha256"], now=NOW)
    assert runs == ["execute", "verify"] and len(calls) == 2  # Actual cleanup runs again at confirmation.
    assert confirmed["confirmed"] and confirmed["input_origin"] == "synthetic fixtures"
    assert confirmed["expires_at"] == descriptor["expires_at"]
    assert authority.confirm(roots["analysis"], value, RUN, ATTEMPT, proof["evidence_sha256"], now=NOW) == confirmed
    with pytest.raises(ValueError, match="actual SSH"):
        authority.confirm(roots["analysis"], value, RUN, ATTEMPT, "f" * 64, now=NOW)


def test_failed_execution_cannot_overwrite_partial_tables_or_accept_an_arbitrary_success_file(prepared, monkeypatch):
    value, roots, _, _, _, _ = prepared
    local = transfer(prepared, "control")

    def fail(*_):
        raise RuntimeError("synthetic Spark failure")

    monkeypatch.setattr(dispatch, "run_spark", fail)
    with pytest.raises(RuntimeError):
        dispatch.execute(roots["control"], value, RUN, ATTEMPT)
    bundle = json.loads(local.path("input.json").read_bytes())
    write_json(local.path("receipt.json"), receipt(bundle))
    with pytest.raises(ValueError, match="interrupted"):
        dispatch.execute(roots["control"], value, RUN, ATTEMPT)
    with pytest.raises(ValueError, match="completed execution"):
        dispatch.verify(roots["control"], value, RUN, ATTEMPT)


def test_interrupted_preparation_reuses_exact_issued_bytes_without_renewal(prepared):
    value, roots, descriptor, _, calls, _ = prepared
    directory = authority.location(roots["analysis"], value, RUN, ATTEMPT)
    (directory / "data/package.json").unlink()
    (directory / "data/input.json").unlink()
    (directory / "issued-cleanup.json").unlink()
    Clock.current = NOW + timedelta(seconds=1)
    assert authority.prepare(roots["analysis"], value, RUN, ATTEMPT) == descriptor
    assert len(calls) == 1 and (directory / "data/input.json").exists()
    assert descriptor["read_until"] == (NOW + timedelta(minutes=15)).isoformat()


def test_changed_generation_and_issued_cleanup_never_borrow_another_authority(prepared):
    value, roots, descriptor, remote, _, _ = prepared
    owner = json.loads(remote.owner.read_bytes())
    owner["generation"] = "c4184e1c-3cbe-4a59-a631-9f6215699c29"
    write_json(remote.owner, owner)
    with pytest.raises(ValueError, match="Authority"):
        authority.prepare(roots["analysis"], value, RUN, ATTEMPT, now=NOW)
    assert not authority.manager(roots["control"], value).owner.exists()


@pytest.mark.parametrize("failure", [False, True])
def test_windows_orchestration_reserves_every_copy_and_verifies_over_its_own_transport(prepared, monkeypatch, failure):
    value, roots, descriptor, _, _, _ = prepared
    calls = []

    def fake_spark(root, directory, desc, mode):
        calls.append(("spark", mode))
        local = RealLifecycle(directory / "data")
        bundle = json.loads(local.path("input.json").read_bytes())
        target = "receipt.json" if mode == "execute" else "verified-engine.json"
        write_json(local.path(target), receipt(bundle, "application_123_0001" if mode == "execute" else "application_123_0002"))
        return dict(bytes=1, retained_bytes=1, truncated=False)

    class SyntheticRunner(dispatch.LakeRunner):
        def stage(self, node, phase, run_id, attempt):
            calls.append((node, "stage-" + phase))

        def phase(self, node, phase, run_id, attempt, evidence_sha=None):
            calls.append((node, phase))
            root = roots["analysis" if node == "snow-analysis" else "control"]
            directory = authority.location(root, value, run_id, attempt)
            if phase == "prepare":
                authority.prepare(root, value, run_id, attempt)
            elif phase == "directories":
                directory.mkdir(parents=True, exist_ok=True)
            elif phase == "reserve":
                authority.reserve(root, value, run_id, attempt, json.loads((directory / "incoming-descriptor.json").read_bytes()))
            elif phase == "accept":
                authority.accept(root, value, run_id, attempt)
            elif phase == "execute":
                dispatch.execute(root, value, run_id, attempt)
            elif phase == "verify":
                dispatch.verify(root, value, run_id, attempt)
            elif phase == "confirm":
                authority.confirm(root, value, run_id, attempt, evidence_sha)
            elif phase == "cancel-driver":
                pass  # Synthetic transport; the dedicated cancellation tests cover ownership.
            else:
                pytest.fail("Unexpected phase")

        def copy(self, node, local, relative, *, upload=False):
            remote = roots["analysis" if node == "snow-analysis" else "control"] / relative
            source, target = (local, remote) if upload else (remote, local)
            if "/data/" in target.as_posix():
                registered = RealLifecycle(target.parent)._read()["artifacts"]
                assert target.name in registered
                assert timestamp(registered[target.name]["expires_at"]) == timestamp(descriptor["expires_at"])
            if failure and upload and relative.endswith("package.json.tmp"):
                target.write_bytes(b"partial synthetic copy")
                raise RuntimeError("synthetic SCP failure")
            shutil.copyfile(source, target)

    monkeypatch.setattr(dispatch, "run_spark", fake_spark)
    runner = SyntheticRunner(value, "runtime/real/config/test.json", roots["operator"])
    if failure:
        with pytest.raises(RuntimeError, match="SCP"):
            runner.lake(RUN, ATTEMPT)
        assert not any(item[0] == "spark" for item in calls)
        data = RealLifecycle(authority.location(roots["control"], value, RUN, ATTEMPT) / "data")
        assert data.path("package.json.tmp").exists()
        assert "package.json.tmp" in data._read()["artifacts"]
    else:
        result = runner.lake(RUN, ATTEMPT)
        assert result["confirmed"] is True
        assert calls[-4] == ("snow-analysis", "confirm")
        assert calls[-3:] == [("snow-compute", "stage-restore"), ("snow-control", "stage-restore"), ("snow-analysis", "stage-restore")]
        assert calls.index(("snow-control", "stage-yarn")) < calls.index(("spark", "execute"))
        assert calls.index(("spark", "verify")) < calls.index(("snow-control", "stage-restore")) < calls.index(("snow-analysis", "confirm"))
        assert ("spark", "execute") in calls and ("spark", "verify") in calls
        assert not authority.manager(roots["operator"], value).registry.exists()
        assert not authority.manager(roots["control"], value).registry.exists()


def test_extra_namespace_or_unlinked_payload_is_rejected(prepared):
    value, roots, descriptor, *_ = prepared
    bad = roots["operator"] / authority.NAMESPACE / "foreign.txt"
    bad.parent.mkdir(parents=True)
    bad.write_text("synthetic unrelated file")
    with pytest.raises(ValueError, match="Unregistered"):
        authority.cleanup_copies(roots["operator"], now=NOW)
    assert bad.read_text() == "synthetic unrelated file"


def test_analysis_phases_borrow_fixed_hive_rpc_without_reverse_ssh(prepared, monkeypatch):
    value, roots, _, *_ = prepared
    calls = []

    class Coordinator:
        def __init__(self, config, config_file, root):
            assert config == value and root == roots["operator"]
            assert config_file == "runtime/real/config/test.json"

        def run(self, operation, run_id, **kwargs):
            calls.append((operation, run_id, kwargs))

    monkeypatch.setitem(sys.modules, "snow_statistics.real_hive_dispatch", SimpleNamespace(HiveCoordinator=Coordinator))
    runner = dispatch.LakeRunner(value, "runtime/real/config/test.json", roots["operator"])
    runner.phase("snow-analysis", "prepare", RUN, ATTEMPT)
    runner.phase("snow-analysis", "confirm", RUN, ATTEMPT, "a" * 64)
    assert calls == [("lake-prepare", RUN, dict(attempt=ATTEMPT, evidence_sha256=None)),
                     ("lake-confirm", RUN, dict(attempt=ATTEMPT, evidence_sha256="a" * 64))]


@pytest.mark.parametrize("exit_code", [0, 9])
def test_fixed_spark_process_caps_logs_and_checks_only_its_exact_container(prepared, monkeypatch, exit_code):
    value, roots, descriptor, *_ = prepared
    local = transfer(prepared, "control")
    directory = local.root.parent
    calls = []
    name = "snow-real-lake-" + digest(canonical(descriptor))[:20]

    def inspect(command, **kwargs):
        calls.append(command)
        assert command == ["sudo", "docker", "inspect", name]
        return subprocess.CompletedProcess(command, 1, b"[]", b"Error: No such object")

    class Process:
        pid = 4321

        def __init__(self, command, **kwargs):
            calls.append(command)
            assert command[:2] == ["bash", "tools/real_lake_spark.sh"]
            assert command[3:5] == [name, descriptor["scope"]]
            assert command[5] == "/opt/snow/warehouse/spark/real_iceberg.py"
            assert kwargs["start_new_session"] is True
            self.stdout = io.BytesIO(b"synthetic log\n" * 100000)

        def wait(self, timeout):
            assert 0 < timeout <= 600
            return exit_code

        def poll(self):
            return exit_code

    monkeypatch.setattr(dispatch, "engine_identity", lambda *a, **k: descriptor["engine"])
    monkeypatch.setattr(dispatch.subprocess, "run", inspect)
    monkeypatch.setattr(dispatch.subprocess, "Popen", Process)
    if exit_code:
        with pytest.raises(RuntimeError, match="application failed"):
            dispatch.run_spark(roots["control"], directory, descriptor, "execute")
    else:
        result = dispatch.run_spark(roots["control"], directory, descriptor, "execute")
        assert result["truncated"] and result["retained_bytes"] == 1024**2
    assert local.path("spark.log").stat().st_size == 1024**2
    assert len(calls) == 4  # inspect -> fixed job -> two exact absence checks, never a prune.


def test_fixed_image_and_hardlinked_files_are_not_accepted(prepared, tmp_path):
    _, roots, _, *_ = prepared
    images = roots["control"] / "lab/locks/images.env"
    images.write_text(images.read_text().replace(authority.SPARK_IMAGE, "apache/spark@sha256:" + "b" * 64))
    with pytest.raises(ValueError, match="pinned Spark"):
        authority.engine_identity(roots["control"])
    source, linked = tmp_path / "source.json", tmp_path / "linked.json"
    source.write_text("{}")
    linked.hardlink_to(source)
    with pytest.raises(ValueError, match="unlinked"):
        authority.read(linked)


def test_failed_process_group_signal_still_cleans_exact_driver(prepared, monkeypatch):
    _, roots, descriptor, *_ = prepared
    local = transfer(prepared, "control")
    cleaned = []
    class Process:
        pid = 4321
        stdout = io.BytesIO(b"")
        def wait(self, timeout):
            raise subprocess.TimeoutExpired("fixed driver", timeout)
        def poll(self):
            return None
    monkeypatch.setattr(dispatch, "engine_identity", lambda *a, **k: descriptor["engine"])
    monkeypatch.setattr(dispatch.subprocess, "run", lambda command, **k: subprocess.CompletedProcess(command, 1, b"", b"No such"))
    monkeypatch.setattr(dispatch.subprocess, "Popen", lambda *a, **k: Process())
    def failed_signal(*unused):
        raise ProcessLookupError("synthetic process disappeared")
    monkeypatch.setattr(dispatch.os, "killpg", failed_signal, raising=False)
    monkeypatch.setattr(dispatch, "_clean_driver", lambda *a: cleaned.append(a[-1]))
    with pytest.raises(ProcessLookupError):
        dispatch.run_spark(roots["control"], local.root.parent, descriptor, "execute")
    assert cleaned == ["snow-real-lake-" + digest(canonical(descriptor))[:20]]


def test_engine_receipts_reject_boolean_counts_and_false_snapshot_types(prepared):
    value, roots, *_ = prepared
    _, data, _ = authority.read_attempt(roots["analysis"], value, RUN, ATTEMPT, now=NOW)
    for field, wrong in (("rows", True), ("original_snapshot", "10")):
        invalid = receipt(data["bundle"])
        invalid["tables"]["daily"][field] = wrong
        with pytest.raises(ValueError, match="unexpected fields"):
            authority.checked_engine_receipt(invalid, data["bundle"], now=NOW)


def test_actual_hive_operation_validator_accepts_the_lake_attempt_contract():
    from snow_statistics.real_hive_dispatch import operation_arguments
    for attempt in ("A", "lake-01", "x" * 60):
        operation_arguments("lake-prepare", RUN, attempt, None)
        operation_arguments("lake-confirm", RUN, attempt, "a" * 64)


def test_driver_cleanup_requires_the_actual_creation_id_not_only_a_label(prepared, monkeypatch):
    _, roots, descriptor, *_ = prepared
    local = transfer(prepared, "control")
    name = "snow-real-lake-" + digest(canonical(descriptor))[:20]
    local.path("driver.cid").write_text("a" * 64)
    calls = []
    def inspect(command, **unused):
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, canonical([dict(Id="b" * 64, Name="/" + name,
            Config=dict(Labels={"org.snow-statistics.lake-driver": name}))]), b"")
    monkeypatch.setattr(dispatch.subprocess, "run", inspect)
    with pytest.raises(ValueError, match="ownership changed"):
        dispatch._clean_driver(roots["control"], local.root.parent, name)
    assert calls == [["sudo", "docker", "inspect", name]]
    assert local.path("driver.cid").exists()


def test_log_thread_start_failure_still_cleans_launched_driver(prepared, monkeypatch):
    _, roots, descriptor, *_ = prepared
    local = transfer(prepared, "control")
    cleaned = []
    process = SimpleNamespace(stdout=io.BytesIO(b""), poll=lambda: 0)
    class Worker:
        ident = None
        def __init__(self, **unused):
            pass
        def start(self):
            raise RuntimeError("synthetic thread startup failure")
    monkeypatch.setattr(dispatch, "engine_identity", lambda *a, **k: descriptor["engine"])
    monkeypatch.setattr(dispatch.subprocess, "run", lambda command, **k: subprocess.CompletedProcess(command, 1, b"", b"No such"))
    monkeypatch.setattr(dispatch.subprocess, "Popen", lambda *a, **k: process)
    monkeypatch.setattr(dispatch.threading, "Thread", Worker)
    monkeypatch.setattr(dispatch, "_clean_driver", lambda *a: cleaned.append(a[-1]))
    with pytest.raises(RuntimeError, match="thread startup"):
        dispatch.run_spark(roots["control"], local.root.parent, descriptor, "execute")
    assert cleaned and process.stdout.closed


def test_cancel_uses_exact_worker_identity_after_expiry_and_prevents_late_admission(prepared, monkeypatch):
    value, roots, descriptor, *_ = prepared
    local = transfer(prepared, "control")
    identity = dict(pid=4321, start_ticks="123456", argv_sha256="a" * 64)
    alive, signals, drivers = [True], [], []
    monkeypatch.setattr(dispatch.os, "getpid", lambda: 4321)
    monkeypatch.setattr(dispatch, "process_identity", lambda pid: identity if pid == 4321 and alive[0] else None)
    with dispatch.node_worker(roots["control"], value, RUN, ATTEMPT, "execute"):
        pass
    def kill(pid, sig):
        signals.append((pid, sig))
        alive[0] = False
    monkeypatch.setattr(dispatch.os, "kill", kill)
    monkeypatch.setattr(dispatch, "_clean_driver", lambda *args: drivers.append(args[-1]))
    Clock.current = timestamp(descriptor["expires_at"]) + timedelta(days=1)
    result = dispatch.cancel_driver(roots["control"], value, RUN, ATTEMPT)
    assert result["status"] == "driver_cancelled" and signals == [(4321, dispatch.signal.SIGTERM)]
    assert drivers and local.path("package.json").exists()  # Cancel does not read/delete the expired payload.
    with pytest.raises(ValueError, match="cancelled"):
        with dispatch.node_worker(roots["control"], value, RUN, ATTEMPT, "verify"):
            pytest.fail("A late worker must not enter")


def test_cancel_never_signals_a_reused_pid(prepared, monkeypatch):
    value, roots, _, *_ = prepared
    local = transfer(prepared, "control")
    write_json(local.root.parent / "node-worker.json", dict(phase="verify", config_sha256=digest(canonical(value)),
               process=dict(pid=4321, start_ticks="123", argv_sha256="a" * 64)))
    monkeypatch.setattr(dispatch, "process_identity", lambda pid: dict(pid=pid, start_ticks="456", argv_sha256="b" * 64))
    monkeypatch.setattr(dispatch.os, "kill", lambda *unused: pytest.fail("Do not signal a reused PID"))
    monkeypatch.setattr(dispatch, "_clean_driver", lambda *unused: None)
    assert dispatch.cancel_driver(roots["control"], value, RUN, ATTEMPT)["status"] == "driver_cancelled"


@pytest.mark.parametrize("failure", ["signal-race", "initial-proc-read", "waiting-proc-read"])
@pytest.mark.parametrize("cleanup_fails", [False, True])
def test_cancel_process_failure_still_cleans_exact_driver_and_preserves_failure(prepared, monkeypatch, failure, cleanup_fails):
    value, roots, descriptor, *_ = prepared
    local = transfer(prepared, "control")
    identity = dict(pid=4321, start_ticks="123456", argv_sha256="a" * 64)
    write_json(local.root.parent / "node-worker.json", dict(phase="verify", config_sha256=digest(canonical(value)), process=identity))
    original = ProcessLookupError("synthetic worker exited before signal") if failure == "signal-race" else OSError("synthetic /proc IO failure")
    cleanup_error = RuntimeError("synthetic exact driver inspect failed")
    reads, signals, cleaned = [], [], []
    def identity_read(pid):
        reads.append(pid)
        if failure == "initial-proc-read" or failure == "waiting-proc-read" and len(reads) == 2:
            raise original
        return identity
    def kill(pid, sig):
        signals.append((pid, sig))
        if failure == "signal-race":
            raise original
    def clean(*args):
        cleaned.append(args)
        if cleanup_fails:
            raise cleanup_error
    monkeypatch.setattr(dispatch, "process_identity", identity_read)
    monkeypatch.setattr(dispatch.os, "kill", kill)
    monkeypatch.setattr(dispatch, "_clean_driver", clean)
    with pytest.raises(type(original)) as caught:
        dispatch.cancel_driver(roots["control"], value, RUN, ATTEMPT)
    assert caught.value is original
    assert cleaned == [(roots["control"], local.root.parent, "snow-real-lake-" + digest(canonical(descriptor))[:20])]
    assert signals == ([] if failure == "initial-proc-read" else [(4321, dispatch.signal.SIGTERM)])
    if cleanup_fails:
        assert caught.value.__cause__ is cleanup_error
    assert local.path("package.json").exists()


@pytest.mark.parametrize("change", [
    lambda value: value.update(config_sha256="b" * 64),
    lambda value: value["process"].update(pid=True),
    lambda value: value["process"].update(start_ticks="unknown"),
    lambda value: value["process"].update(argv_sha256="invalid"),
])
def test_cancel_rejects_untrusted_worker_before_any_process_or_driver_action(prepared, monkeypatch, change):
    config, roots, *_ = prepared
    local = transfer(prepared, "control")
    worker = dict(phase="execute", config_sha256=digest(canonical(config)),
                  process=dict(pid=4321, start_ticks="123456", argv_sha256="a" * 64))
    change(worker)
    write_json(local.root.parent / "node-worker.json", worker)
    monkeypatch.setattr(dispatch, "process_identity", lambda *a: pytest.fail("Untrusted worker must not be probed"))
    monkeypatch.setattr(dispatch, "_clean_driver", lambda *a: pytest.fail("Untrusted worker must not mutate a driver"))
    with pytest.raises(ValueError, match="ownership or identity"):
        dispatch.cancel_driver(roots["control"], config, RUN, ATTEMPT)
    assert not (local.root.parent / "cancelled.json").exists()


@pytest.mark.parametrize("platform", ["nt", "posix"])
def test_owned_transport_timeout_stops_its_process_tree(prepared, monkeypatch, platform):
    value, roots, *_ = prepared
    calls = []
    process = SimpleNamespace(pid=4321, returncode=None, poll=lambda: process.returncode)
    def wait(timeout):
        if process.returncode is None:
            raise subprocess.TimeoutExpired("owned wrapper", timeout)
        return process.returncode
    process.wait = wait
    def kill(*args, **kwargs):
        calls.append(args)
        process.returncode = -9
    monkeypatch.setattr(dispatch, "os", SimpleNamespace(name=platform, killpg=kill))
    monkeypatch.setattr(dispatch.subprocess, "CREATE_NEW_PROCESS_GROUP", 512, raising=False)
    monkeypatch.setattr(dispatch.subprocess, "CREATE_NO_WINDOW", 1024, raising=False)
    monkeypatch.setattr(dispatch.signal, "SIGKILL", 9, raising=False)
    monkeypatch.setattr(dispatch.subprocess, "Popen", lambda *args, **kwargs: process)
    monkeypatch.setattr(dispatch.subprocess, "run", kill)
    runner = dispatch.LakeRunner(value, "runtime/real/config/test.json", roots["operator"])
    with pytest.raises(subprocess.TimeoutExpired):
        runner.run(["python", "owned-wrapper"], timeout=1)
    expected = [(["taskkill", "/PID", "4321", "/T", "/F"],)] if platform == "nt" else [(4321, dispatch.signal.SIGKILL)]
    assert calls == expected


def test_driver_absence_readback_failure_preserves_the_cid(prepared, monkeypatch):
    _, roots, descriptor, *_ = prepared
    local = transfer(prepared, "control")
    name = "snow-real-lake-" + digest(canonical(descriptor))[:20]
    local.path("driver.cid").write_text("a" * 64)
    image = canonical([dict(Id="a" * 64, Name="/" + name, Config=dict(Labels={"org.snow-statistics.lake-driver": name}))])
    calls = []
    def run(command, **unused):
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, image, b"")
    monkeypatch.setattr(dispatch.subprocess, "run", run)
    with pytest.raises(RuntimeError, match="absence was not confirmed"):
        dispatch._clean_driver(roots["control"], local.root.parent, name)
    assert local.path("driver.cid").exists()
    assert calls[1] == ["sudo", "docker", "rm", "-f", "a" * 64]


def test_private_view_cleans_expired_lake_copy_before_missing_ods_gate(prepared, monkeypatch):
    from snow_statistics import real_aggregate_read as aggregate
    value, roots, descriptor, *_ = prepared
    local = transfer(prepared, "operator")
    class Root:
        def __str__(self):
            return "/home/snow/Snow_Statistics"
        def __fspath__(self):
            return str(roots["operator"])
        def __truediv__(self, child):
            return roots["operator"] / child
    monkeypatch.setattr(aggregate, "os", SimpleNamespace(name="posix"))
    monkeypatch.setattr(aggregate, "socket", SimpleNamespace(gethostname=lambda: "snow-analysis"))
    Clock.current = timestamp(descriptor["expires_at"]) + timedelta(days=1)
    with pytest.raises(ValueError, match="missing durable ODS"):
        aggregate.read_managed_aggregate(value, RUN, root=Root())
    assert not local.path("package.json").exists()
    bad = roots["operator"] / authority.NAMESPACE / "unregistered.txt"
    bad.write_text("synthetic unexpected artifact")
    with pytest.raises(ValueError, match="Unregistered"):
        aggregate.read_managed_aggregate(value, RUN, root=Root())
    assert bad.exists()  # No read admission and no deletion of an unknown file.


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Actual /proc worker identity requires Linux")
def test_linux_process_identity_binds_start_ticks_and_argv_then_detects_exit():
    process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(1)"],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        value = dispatch.process_identity(process.pid)
        assert value["pid"] == process.pid and value["start_ticks"].isdigit() and len(value["argv_sha256"]) == 64
    finally:
        process.terminate()
        process.wait(timeout=5)
    assert dispatch.process_identity(process.pid) != value
