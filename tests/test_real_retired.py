"""Synthetic Docker API fixtures; never physical production deletion evidence."""
import copy
from datetime import UTC, datetime, timedelta
from urllib.parse import urlsplit

import pytest
import test_real_quiescent as writer_fixture
from test_real_remote_lifecycle import FixtureHdfs

from snow_statistics.io import digest, write_json
from snow_statistics.publication import canonical
from snow_statistics.real_epoch import LABEL_PREFIX
from snow_statistics.real_quiescent import backend_resources
from snow_statistics.real_remote_lifecycle import RealRemoteLifecycle
from snow_statistics.real_retired import RetiredStorage, archived_writer, validate_retired_receipt


def expired(tmp_path, monkeypatch):
    at = datetime.now(UTC) - timedelta(days=8)
    monkeypatch.setattr(writer_fixture, "NOW", at)
    registry, docker, _ = writer_fixture.candidate(tmp_path, now=at)
    value = registry.read(now=at)
    identity = value["initial"]["kafka"]["identity"]
    topic = next(name for name in identity["topic_ids"] if name.endswith(".events.v1"))
    snap = dict(schema_version=2, source="real", identity={"collector": value["collector"], "cluster_id": identity["cluster_id"],
                "topic_ids": {topic: identity["topic_ids"][topic]}}, offsets={topic + ":0": 5}, batches=[],
                root="hdfs://snow-control:9000/snow/ods/real/kafka/candidate-one", head_batch_id="a" * 64)
    return RetiredStorage(registry.epoch, snap), registry, docker, value, at


def test_actual_retirement_and_fresh_absence_allow_only_aggregate_scope(tmp_path, monkeypatch):
    retired, registry, docker, value, _ = expired(tmp_path, monkeypatch)
    # Historical code/JAR availability cannot determine whether physically gone
    # storage can ever make a still-live private aggregate readable again.
    (tmp_path / "fixture.jar").unlink()
    monkeypatch.setattr("snow_statistics.real_quiescent.writer_hashes", lambda: pytest.fail("must not check live writer hashes"))
    first = retired.check()
    assert not docker.c and not docker.v
    assert first["verification"] == "retired_storage_absent" and first["physical_storage_absent"] is True
    assert first["raw_compute_allowed"] is first["writer_restore_allowed"] is False
    second = retired.check()
    assert first["retirement_sha256"] == second["retirement_sha256"]
    resources = backend_resources(value)
    now = datetime.now(UTC)
    receipt = retired.adapters()["kafka"].verify_retired(resources["kafka"], now)
    owner = {key: value["collector"][key] for key in ("instance_id", "generation")}
    assert validate_retired_receipt(receipt, "kafka", resources["kafka"], owner, retired.snapshot, now)["next_expiry"] is None
    with pytest.raises((ValueError, FileNotFoundError)):
        registry.ready()
    with pytest.raises(ValueError, match="restart"):
        registry.epoch.start()


@pytest.mark.parametrize("kind", ["container", "volume", "alias_container", "alias_volume", "prefix", "compose_label"])
def test_same_name_or_unknown_same_epoch_objects_close_gate(tmp_path, monkeypatch, kind):
    retired, registry, docker, _, _ = expired(tmp_path, monkeypatch)
    retired.check()
    manifest = registry.epoch.read()
    if kind == "container":
        docker.c[manifest["containers"]["kafka"]] = {"labels": {"unrelated": "owner"}}
    elif kind == "volume":
        docker.v[manifest["volumes"]["kafka"]] = {"Labels": {"unrelated": "owner"}}
    elif kind == "alias_container":
        docker.c["unknown-copy"] = {"labels": {LABEL_PREFIX + "epoch": manifest["epoch_id"]}, "mounts": []}
    elif kind == "alias_volume":
        docker.v["unknown-copy"] = {"Labels": {LABEL_PREFIX + "generation": manifest["generation"]}}
    elif kind == "prefix":
        docker.v[manifest["project"] + "-unknown"] = {"Labels": {}}
    else:
        docker.c["unknown-copy"] = {"labels": {"com.docker.compose.project": manifest["project"]}, "mounts": []}
    before = list(docker.actions)
    with pytest.raises(ValueError, match="reappeared|unregistered"):
        retired.check()
    assert docker.actions == before


def test_unknown_copy_blocks_before_first_retirement_mutation(tmp_path, monkeypatch):
    retired, registry, docker, _, _ = expired(tmp_path, monkeypatch)
    docker.v["hidden-copy"] = {"Labels": {LABEL_PREFIX + "epoch": registry.epoch.read()["epoch_id"]}}
    before = list(docker.actions)
    with pytest.raises(ValueError, match="unregistered"):
        retired.check()
    assert docker.actions == before


@pytest.mark.parametrize("mutation", [
    lambda v: v.update(evidence_sha256="0" * 64),
    lambda v: v.update(input_origin="synthetic fixtures"),
    lambda v: v.update(generation="00000000-0000-0000-0000-000000000001"),
    lambda v: v.update(checked_at="2099-01-01T00:00:00Z"),
    lambda v: v.update(forensic_media_erasure_claimed=True),
    lambda v: v["readback_absent"]["volumes"].pop(),
    lambda v: v["containers_removed"].append("business-container"),
    lambda v: v.update(success=True),
])
def test_retirement_json_is_not_an_approval_channel(tmp_path, monkeypatch, mutation):
    retired, registry, _, _, _ = expired(tmp_path, monkeypatch)
    retired.check()
    from snow_statistics.real_quiescent import bounded_json
    path = registry.epoch.directory / "retirement.json"
    receipt = bounded_json(path)
    mutation(receipt)
    write_json(path, receipt)
    with pytest.raises(ValueError):
        retired.check()


def test_failed_physical_delete_and_changed_archived_identity_refuse_reads(tmp_path, monkeypatch):
    retired, registry, docker, value, _ = expired(tmp_path, monkeypatch)
    docker.fail_volume = True
    with pytest.raises(RuntimeError, match="remains"):
        retired.check()
    assert not (registry.epoch.directory / "retirement.json").exists()
    docker.fail_volume = False
    retired.check()
    bad = copy.deepcopy(retired.snapshot)
    bad["identity"]["collector"]["generation"] = "00000000-0000-0000-0000-000000000003"
    with pytest.raises(ValueError, match="differs"):
        archived_writer(registry.epoch, bad)
    value["initial"]["kafka"]["bounds"][next(iter(value["initial"]["kafka"]["bounds"]))]["end"] = 1
    write_json(registry.path, value)
    with pytest.raises(ValueError):
        retired.check()


def test_live_epoch_and_fixtures_cannot_use_retired_admission(tmp_path):
    registry, _, _ = writer_fixture.candidate(tmp_path)
    with pytest.raises(ValueError, match="actually expired"):
        RetiredStorage(registry.epoch, {}).check()


class EmptyOds:
    def __init__(self, directory, snapshot):
        self.root, self.body = snapshot["root"], canonical(snapshot)
        self.input = self.root + "/snapshots/" + digest(self.body) + "/_snapshot.json"
        write_json(directory / "state.json", snapshot | dict(snapshot_id=digest(self.body), input=self.input, batch_id=snapshot["head_batch_id"]))

    def read(self, path):
        assert path == urlsplit(self.input).path
        return self.body


def test_remote_cleanup_certifies_retired_scope_without_zero_or_live_compute(tmp_path, monkeypatch):
    retired, registry, docker, value, at = expired(tmp_path, monkeypatch)
    manager = RealRemoteLifecycle(tmp_path / "remote")
    warehouse = "hdfs://snow-control:9000/snow/warehouse/real/candidate-one"
    auxiliary = "hdfs://snow-control:9000/snow/auxiliary/real/candidate-one"
    manager.initialize(warehouse, auxiliary, retired.snapshot["root"], value["collector"]["instance_id"], value["collector"]["generation"])
    for backend, resources in backend_resources(value).items():
        for name, entry in resources.items():
            manager.register_backend(backend, name, entry["kind"], entry["original_min_accepted_at"], now=at)
    path = warehouse + "/runs/preserved/ads_daily"
    manager.register(path, "aggregate", at.isoformat())
    hdfs = FixtureHdfs()
    hdfs.files.add(path + "/part-0001.parquet")
    ods = tmp_path / "ods"
    ods.mkdir()
    sink = EmptyOds(ods, retired.snapshot)
    receipt = manager.cleanup(hdfs, ods, sink, backend_checks=retired.adapters())
    assert hdfs.exists(path) and not docker.c and not docker.v
    assert all(receipt["backends"][name]["verification"] == "retired_storage_absent" for name in ("kafka", "doris", "checkpoint"))
    assert receipt["backends"]["hive"] == {"scope": "not_initialized", "certified": False}
    # Even a newly supplied live reservation cannot use retired storage as a
    # shortcut into raw/aux compute. Original public/private aggregates persist.
    day = datetime.now(UTC).date().isoformat()
    job = dict(run_id="blocked", source="real", input=sink.input, warehouse_root=warehouse, auxiliary_root=auxiliary,
               date_from=day, date_to=day, cutoff=datetime.now(UTC).isoformat())
    current = datetime.now(UTC)
    manager.reserve_job(job, current.isoformat(), current.isoformat())
    coverage = tmp_path / "coverage.json"
    write_json(coverage, dict(schema_version=1, source="real", **{k: value["collector"][k] for k in ("instance_id", "generation")},
                              continuous_from=job["cutoff"], through=job["cutoff"], gaps=[]))
    with pytest.raises(ValueError, match="aggregate reads only"):
        manager.issue_permit(job, coverage, None, tmp_path / "permit.json", hdfs, ods, sink, backend_checks=retired.adapters())
    assert not (tmp_path / "permit.json").exists()


def test_partial_or_fake_retired_adapter_cannot_skip_registered_backends(tmp_path, monkeypatch):
    retired, _, _, value, at = expired(tmp_path, monkeypatch)
    manager = RealRemoteLifecycle(tmp_path / "remote")
    manager.initialize("hdfs://snow-control:9000/snow/warehouse/real/candidate-one", "hdfs://snow-control:9000/snow/auxiliary/real/candidate-one",
                       retired.snapshot["root"], value["collector"]["instance_id"], value["collector"]["generation"])
    for name, entry in backend_resources(value)["kafka"].items():
        manager.register_backend("kafka", name, entry["kind"], entry["original_min_accepted_at"], now=at)
    ods = tmp_path / "ods"
    ods.mkdir()
    sink = EmptyOds(ods, retired.snapshot)
    with pytest.raises(ValueError, match="all engine backends"):
        manager.cleanup(FixtureHdfs(), ods, sink, backend_checks={"kafka": retired.adapters()["kafka"]})
