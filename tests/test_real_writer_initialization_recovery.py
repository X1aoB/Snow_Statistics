"""Recovery uses real registry code with synthetic engine readback adapters."""
import copy
import json
import os
from datetime import UTC, datetime

import pytest
from test_real_quiescent import COLLECTOR, candidate
from test_real_writer_bootstrap import synthetic_bootstrap

from snow_statistics import real_writer as module
from snow_statistics.io import write_json


def interrupted(tmp_path, monkeypatch):
    registry, docker, probe = candidate(tmp_path, registered=False)
    writer = object.__new__(module.ActualWriter)
    writer.epoch, writer.registry = registry.epoch, registry
    writer.root = registry.epoch.directory.parent
    writer.manifest = registry.epoch.read()
    writer.directory = registry.epoch.directory / "writer"
    writer.directory.mkdir(mode=0o700)
    writer.identity = lambda: copy.deepcopy(COLLECTOR)
    writer.read_initial_state = probe.read_initial_state
    writer.sql = lambda *a, **k: pytest.fail("Recovery must not execute DDL/account changes")
    write_json(writer.directory / "initializing.json", dict(source="real", started_at=datetime.now(UTC).isoformat()))
    write_json(writer.directory / "account.json", dict(user="sr_" + writer.manifest["event_lane"], password="synthetic-private-fixture"))
    os.chmod(writer.directory / "account.json", 0o600)
    monkeypatch.setattr(module, "expire_due", lambda *args: None)
    return writer, docker, probe


def test_finalize_reads_empty_engines_preserves_originals_and_never_runs_ddl(tmp_path, monkeypatch):
    writer, _, probe = interrupted(tmp_path, monkeypatch)
    originals = {name: (writer.directory / name).read_bytes() for name in ("initializing.json", "account.json")}
    manifest = writer.epoch.read()
    result = writer.finalize_initialization()
    assert result["initial"] == probe.initial
    assert result["expires_at"] == manifest["expires_at"]
    assert writer.epoch.read() == manifest
    assert all((writer.directory / name).read_bytes() == body for name, body in originals.items())
    receipt = json.loads((writer.directory / "initialization-finalization.json").read_bytes())
    assert receipt["ddl_executed"] is False and receipt["original_files_preserved"] is True
    assert receipt["original_expires_at"] == manifest["expires_at"]
    assert len(probe.reads) == 2  # Before intent and again within immutable registration.
    assert not writer.registry.job_path.exists()
    with pytest.raises(ValueError, match="already completed"):
        writer.finalize_initialization()


@pytest.mark.parametrize("kind", ["kafka", "doris", "job", "checkpoint", "submission", "unowned_registration", "marker"])
def test_finalize_refuses_payload_or_unowned_partial_state(tmp_path, monkeypatch, kind):
    writer, _, probe = interrupted(tmp_path, monkeypatch)
    if kind == "kafka":
        next(iter(probe.initial["kafka"]["bounds"].values()))["end"] = 1
    elif kind == "doris":
        probe.initial["doris"]["tables"]["events_realtime"] = 1
    elif kind == "job":
        probe.initial["flink"]["jobs"] = [dict(jid="a" * 32)]
    elif kind == "checkpoint":
        probe.initial["state"]["/checkpoints"] = ["unexpected"]
    elif kind == "submission":
        write_json(writer.directory / "submission.json", {"source": "real"})
    elif kind == "unowned_registration":
        writer.registry.initialize(COLLECTOR, probe)
    else:
        write_json(writer.directory / "initializing.json", {"source": "synthetic", "started_at": datetime.now(UTC).isoformat()})
    with pytest.raises(ValueError):
        writer.finalize_initialization()
    assert not (writer.directory / "initialization-finalization-intent.json").exists()
    assert not (writer.directory / "initialization-finalization.json").exists()


def test_crash_after_registration_resumes_only_the_bound_original_intent(tmp_path, monkeypatch):
    writer, _, probe = interrupted(tmp_path, monkeypatch)
    probe.initial["bootstrap"] = synthetic_bootstrap()
    original_write = module.write_json
    def crash_after_registration(path, value):
        if path.name == "initialization-finalization.json":
            raise OSError("synthetic process interruption before completion metadata")
        return original_write(path, value)
    monkeypatch.setattr(module, "write_json", crash_after_registration)
    with pytest.raises(OSError):
        writer.finalize_initialization()
    registration = writer.registry.path.read_bytes()
    intent = (writer.directory / "initialization-finalization-intent.json").read_bytes()
    # A JVM restart may regenerate a verified bootstrap filename. It cannot
    # replace payload, storage identity or extend the original data window.
    probe.initial["bootstrap"]["files"][0]["path"] = "/flink-state/tmp/jaas-789.conf"
    monkeypatch.setattr(module, "write_json", original_write)
    writer.finalize_initialization()
    assert writer.registry.path.read_bytes() == registration
    assert (writer.directory / "initialization-finalization-intent.json").read_bytes() == intent


@pytest.mark.parametrize("kind", ["account", "storage", "collector", "code"])
def test_interrupted_intent_cannot_rebind_identity(tmp_path, monkeypatch, kind):
    writer, docker, _ = interrupted(tmp_path, monkeypatch)
    original_initialize = writer.registry.initialize
    monkeypatch.setattr(writer.registry, "initialize", lambda *a: (_ for _ in ()).throw(OSError("synthetic interruption")))
    with pytest.raises(OSError):
        writer.finalize_initialization()
    monkeypatch.setattr(writer.registry, "initialize", original_initialize)
    if kind == "account":
        account_file = writer.directory / "account.json"
        value = json.loads(account_file.read_bytes())
        write_json(account_file, value | {"password": "different-synthetic-fixture"})
        os.chmod(account_file, 0o600)
    elif kind == "storage":
        next(iter(docker.c.values()))["object_id"] = "f" * 64
    elif kind == "collector":
        writer.identity = lambda: COLLECTOR | {"generation": "00000000-0000-0000-0000-000000000099"}
    else:
        monkeypatch.setattr(module, "writer_hashes", lambda: {"synthetic-code": "b" * 64})
    with pytest.raises(ValueError, match="immutable intent"):
        writer.finalize_initialization()
    assert not writer.registry.path.exists()
