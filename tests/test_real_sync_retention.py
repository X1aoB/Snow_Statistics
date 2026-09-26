"""Synthetic temporary files only; no engine, collector or system crash access."""
import json
import os
import runpy
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_real_epoch import Docker

from snow_statistics import real_epoch
from snow_statistics.io import digest, write_json
from snow_statistics.lifecycle import RealLifecycle
from snow_statistics.publication import canonical, publication_lock
from snow_statistics.real_sync_retention import cleanup_epoch_sync

NOW = datetime.now(UTC)
COLLECTOR = dict(schema_version=1, source="real", instance_id="00000000-0000-0000-0000-000000000001",
                 generation="00000000-0000-0000-0000-000000000002")
IMAGE = "fixture@sha256:" + "a" * 64


def candidate(tmp_path, *, expired=True, synthetic=False, data=True):
    original = NOW - timedelta(days=8 if expired else 1)
    name = "fixture-synthetic" if synthetic else "real-test-01"
    images = {"PYTHON_IMAGE": IMAGE} if synthetic else {
        key: IMAGE for key in ("KAFKA_IMAGE", "DORIS_FE_IMAGE", "DORIS_BE_IMAGE", "FLINK_IMAGE")}
    manifest = real_epoch.make_manifest(name, original.isoformat(), images, fixture=synthetic,
                                        now=original + timedelta(hours=1))
    epoch = tmp_path / "runtime/real/epochs" / name
    write_json(epoch / "manifest.json", manifest)
    epoch.parent.chmod(0o700)
    sync = epoch.parent.parent / "sync" / name
    if not data:
        return epoch, sync, manifest, None
    write_json(sync / "target.json", dict(schema_version=1, source="real", lane=manifest["event_lane"],
                                          url="http://127.0.0.1:18100", bootstrap="127.0.0.1:19092"))
    write_json(sync / "source.json", COLLECTOR)
    registration = dict(schema_version=1, source="real", input_origin="real", epoch_id=name,
                        epoch_generation=manifest["generation"], owner_manifest_sha256=digest(canonical(manifest)),
                        original_min_accepted_at=manifest["original_min_accepted_at"], expires_at=manifest["expires_at"],
                        collector=COLLECTOR)
    write_json(epoch / "writer-registration.json", registration)
    local = RealLifecycle(sync / "data")
    local.initialize()
    payload = local.register("diagnostics/synthetic-worker.crash", "raw", original.isoformat(),
                             now=original + timedelta(hours=1))
    payload.parent.mkdir()
    payload.write_bytes(b"Synthetic diagnostic fixture; no core or user data")
    return epoch, sync, manifest, payload


def test_expired_actual_file_removed_without_renewing_its_deadline(tmp_path):
    epoch, sync, manifest, payload = candidate(tmp_path)
    before = json.loads((sync / "data/registry.json").read_bytes())["artifacts"]["diagnostics/synthetic-worker.crash"]
    assert before["expires_at"] == manifest["expires_at"]
    result = cleanup_epoch_sync(epoch, manifest)
    assert result["removed"] == 1 and result["open"] is True and not payload.exists()
    assert json.loads((sync / "data/registry.json").read_bytes())["artifacts"] == {}
    assert cleanup_epoch_sync(epoch, manifest)["removed"] == 0


def test_unexpired_payload_and_registration_are_unchanged(tmp_path):
    epoch, sync, manifest, payload = candidate(tmp_path, expired=False)
    registry = (sync / "data/registry.json").read_bytes()
    original = payload.read_bytes()
    assert cleanup_epoch_sync(epoch, manifest)["removed"] == 0
    assert payload.read_bytes() == original and (sync / "data/registry.json").read_bytes() == registry


def test_collector_generation_is_separate_from_epoch_generation(tmp_path):
    epoch, _, manifest, payload = candidate(tmp_path)
    assert COLLECTOR["generation"] != manifest["generation"]
    cleanup_epoch_sync(epoch, manifest)
    assert not payload.exists()


def test_missing_data_is_noop_and_creates_no_registry(tmp_path):
    epoch, sync, manifest, _ = candidate(tmp_path, data=False)
    assert cleanup_epoch_sync(epoch, manifest) is None
    assert not sync.parent.exists()
    sync.mkdir(parents=True)
    assert cleanup_epoch_sync(epoch, manifest) is None and list(sync.iterdir()) == []


def test_existing_unregistered_data_is_not_adopted(tmp_path):
    epoch, sync, manifest, _ = candidate(tmp_path, data=False)
    (sync / "data").mkdir(parents=True)
    with pytest.raises(ValueError, match="missing ownership"):
        cleanup_epoch_sync(epoch, manifest)
    assert not (sync / "data/.snow-real-owner.json").exists()


def test_synthetic_and_other_sync_directories_remain_untouched(tmp_path):
    epoch, _, manifest, payload = candidate(tmp_path, synthetic=True)
    unrelated = tmp_path / "runtime/real/sync/foreign/data/unknown.bin"
    unrelated.parent.mkdir(parents=True)
    unrelated.write_bytes(b"synthetic unrelated scope")
    assert cleanup_epoch_sync(epoch, manifest) is None
    assert payload.exists() and unrelated.exists()


@pytest.mark.parametrize("filename,field,value", [
    ("target.json", "source", "synthetic"), ("target.json", "lane", "other_lane"),
    ("target.json", "schema_version", True), ("target.json", "url", "http://name:secret@example.invalid"),
    ("source.json", "generation", "00000000-0000-0000-0000-000000000003"),
    ("source.json", "source", "synthetic"),
])
def test_mismatched_sync_identity_preserves_payload(tmp_path, filename, field, value):
    epoch, sync, manifest, payload = candidate(tmp_path)
    path = sync / filename
    content = json.loads(path.read_bytes())
    content[field] = value
    write_json(path, content)
    with pytest.raises(ValueError):
        cleanup_epoch_sync(epoch, manifest)
    assert payload.exists()


@pytest.mark.parametrize("field,value", [("epoch_id", "other-epoch"), ("epoch_generation", "wrong"),
                                         ("owner_manifest_sha256", "f" * 64), ("expires_at", "2099-01-01T00:00:00+00:00")])
def test_mismatched_writer_registration_does_not_delete(tmp_path, field, value):
    epoch, _, manifest, payload = candidate(tmp_path)
    path = epoch / "writer-registration.json"
    registration = json.loads(path.read_bytes())
    registration[field] = value
    write_json(path, registration)
    with pytest.raises(ValueError, match="registration differ"):
        cleanup_epoch_sync(epoch, manifest)
    assert payload.exists()


def test_unknown_file_closes_gate_and_preserves_all_files(tmp_path):
    epoch, sync, manifest, payload = candidate(tmp_path)
    unknown = sync / "data/unknown.bin"
    unknown.write_bytes(b"synthetic unknown")
    with pytest.raises(ValueError, match="Unregistered real payload"):
        cleanup_epoch_sync(epoch, manifest)
    assert unknown.exists() and payload.exists()
    assert json.loads((sync / "data/gate.json").read_bytes())["open"] is False


def test_sync_lock_contention_prevents_cleanup(tmp_path):
    epoch, sync, manifest, payload = candidate(tmp_path)
    with publication_lock(sync), pytest.raises(OSError):
        cleanup_epoch_sync(epoch, manifest)
    assert payload.exists()
    cleanup_epoch_sync(epoch, manifest)
    assert not payload.exists()


def test_cleanup_failure_prevents_epoch_retirement(tmp_path, monkeypatch):
    epoch, _, _, payload = candidate(tmp_path)
    monkeypatch.setattr(RealLifecycle, "cleanup", lambda *_: (_ for _ in ()).throw(OSError("synthetic cleanup IO")))
    monkeypatch.setattr(real_epoch.Epoch, "retire", lambda *_: pytest.fail("Must not retire after local cleanup failure"))
    with pytest.raises(OSError, match="synthetic cleanup IO"):
        real_epoch.expire_due(epoch.parent, Docker())
    assert payload.exists()


def test_sync_lock_is_released_before_epoch_retirement(tmp_path, monkeypatch):
    epoch, sync, _, payload = candidate(tmp_path)
    def retire(self):
        with publication_lock(sync):
            assert not payload.exists()
        return {"retired": True}
    monkeypatch.setattr(real_epoch.Epoch, "retire", retire)
    assert real_epoch.expire_due(epoch.parent, Docker()) == [{"retired": True}]


@pytest.mark.parametrize("part", ["data", "source.json", "data/registry.json", "data/publisher.lock"])
def test_linked_directory_or_metadata_is_refused(tmp_path, part):
    epoch, sync, manifest, payload = candidate(tmp_path)
    path = sync / part
    other = tmp_path / ("other-" + path.name)
    if path.exists():
        path.rename(other)
    else:
        other.write_bytes(b"")
    try:
        path.symlink_to(other, target_is_directory=other.is_dir())
    except OSError as error:
        pytest.skip("Host cannot create test symlinks: " + str(error))
    with pytest.raises(ValueError, match="link"):
        cleanup_epoch_sync(epoch, manifest)
    assert (other / "diagnostics/synthetic-worker.crash").exists() if other.is_dir() else payload.exists()


def test_start_cli_uses_cleanup_before_refusing_expired_epoch(tmp_path, monkeypatch):
    epoch, _, _, payload = candidate(tmp_path)
    monkeypatch.setattr(real_epoch, "DockerEpoch", Docker)
    monkeypatch.setattr(sys, "argv", ["tools/real_epoch.py", "--root", str(epoch.parent), "start", "--epoch", epoch.name])
    with pytest.raises(ValueError, match="Expired or retired"):
        runpy.run_path(str(Path(__file__).parents[1] / "tools/real_epoch.py"), run_name="__main__")
    assert not payload.exists() and (epoch / "retirement.json").exists()


def test_watch_uses_same_cleanup_and_does_not_skip_retired_registry(tmp_path, monkeypatch):
    epoch, _, _, payload = candidate(tmp_path)
    monkeypatch.setattr(real_epoch, "signal", SimpleNamespace(signal=lambda *_: None, SIGTERM=15, SIGINT=2))
    monkeypatch.setattr(real_epoch.time, "sleep", lambda _: (_ for _ in ()).throw(SystemExit(0)))
    with pytest.raises(SystemExit):
        real_epoch.supervise(epoch.parent, Docker())
    assert not payload.exists() and (epoch / "retirement.json").exists()
    assert real_epoch.expire_due(epoch.parent, Docker())[0]["volumes_removed"] == []


@pytest.mark.parametrize("part", ["target.json", "source.json", "data/.snow-real-owner.json", "data/registry.json"])
def test_missing_required_metadata_fails_closed(tmp_path, part):
    epoch, sync, manifest, payload = candidate(tmp_path)
    (sync / part).unlink()
    with pytest.raises(ValueError, match="missing ownership"):
        cleanup_epoch_sync(epoch, manifest)
    assert payload.exists()


@pytest.mark.parametrize("part", ["target.json", "source.json", "data/.snow-real-owner.json", "data/registry.json"])
def test_hardlinked_metadata_is_not_a_unique_owned_control_file(tmp_path, part):
    epoch, sync, manifest, payload = candidate(tmp_path)
    path = sync / part
    os.link(path, tmp_path / ("alias-" + path.name))
    with pytest.raises(ValueError, match="unlinked regular"):
        cleanup_epoch_sync(epoch, manifest)
    assert payload.exists()


def test_changed_actual_manifest_is_rejected_before_cleanup(tmp_path):
    epoch, _, manifest, payload = candidate(tmp_path)
    write_json(epoch / "manifest.json", manifest | {"generation": "00000000-0000-0000-0000-000000000004"})
    with pytest.raises(ValueError, match="Actual epoch manifest changed"):
        cleanup_epoch_sync(epoch, manifest)
    assert payload.exists()


def test_extended_artifact_expiry_is_rejected_not_repaired(tmp_path):
    epoch, sync, manifest, payload = candidate(tmp_path)
    path = sync / "data/registry.json"
    registry = json.loads(path.read_bytes())
    registry["artifacts"]["diagnostics/synthetic-worker.crash"]["expires_at"] = "2099-01-01T00:00:00+00:00"
    write_json(path, registry)
    with pytest.raises(ValueError, match="extend retention"):
        cleanup_epoch_sync(epoch, manifest)
    assert payload.exists()
