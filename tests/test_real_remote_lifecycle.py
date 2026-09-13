import json
from datetime import UTC, datetime, timedelta
from urllib.parse import urlsplit

import pytest

from snow_statistics.io import digest, write_json
from snow_statistics.publication import canonical
from snow_statistics.real_remote_lifecycle import RealRemoteLifecycle

NOW = datetime(2026, 9, 14, tzinfo=UTC)
WAREHOUSE = "hdfs://snow-control:9000/snow/warehouse/real/lifecycle_fixture"
AUX = "hdfs://snow-control:9000/snow/auxiliary/real/lifecycle_fixture"
ODS = "hdfs://snow-control:9000/snow/ods/real/kafka/lifecycle-fixture"
IDENTITY = {"instance_id": "fbf85904-0a98-44f4-bd13-804280831449", "generation": "c2b34a4a-a18d-47ac-bfe5-acb41444e3d4"}


class FixtureHdfs:
    """Synthetic metadata backend; not HDFS integration evidence."""
    def __init__(self):
        self.files = set()
        self.deleted = []
        self.fail_once = False

    def exists(self, uri):
        return uri in self.files or any(path.startswith(uri + "/") for path in self.files)

    def children(self, uri):
        result = {}
        for file in self.files:
            if file.startswith(uri + "/"):
                child, _, extra = file[len(uri) + 1:].partition("/")
                result[uri + "/" + child] = "DIRECTORY" if extra else "FILE"
        return sorted(result.items())

    def delete_exact(self, uri):
        self.deleted.append(uri)
        if self.fail_once:
            self.fail_once = False
            raise RuntimeError("injected HDFS outage")
        self.files = {path for path in self.files if path != uri and not path.startswith(uri + "/")}


class FixtureOds:
    def __init__(self, directory):
        self.root = ODS
        snapshot = dict(schema_version=2, source="real", identity={"collector": {"schema_version": 1, "source": "real", **IDENTITY}}, offsets={"snow.real.fixture.events.v1:0": 1},
                        batches=[dict(batch_id="a" * 64, files={}, counts={"events": 1, "quarantine": 0},
                                      original_min_accepted_at=(NOW - timedelta(days=1)).isoformat(),
                                      expires_at=(NOW + timedelta(days=6)).isoformat())], root=ODS, head_batch_id="a" * 64)
        token = digest(canonical(snapshot))
        self.body = canonical(snapshot)
        self.input = ODS + "/snapshots/" + token + "/_snapshot.json"
        write_json(directory / "state.json", snapshot | dict(snapshot_id=token, batch_id="a" * 64, input=self.input))

    def read(self, path):
        assert path == urlsplit(self.input).path
        return self.body


class CheckedBackend:
    def __init__(self, name):
        self.name, self.calls, self.forged = name, 0, False

    def purge_and_verify(self, resources, now):
        self.calls += 1
        return dict(backend=self.name, resources_sha256="0" * 64 if self.forged else digest(canonical(resources)),
                    checked_at=now.isoformat(), remaining_expired=0, evidence_sha256=digest(b"synthetic verification fixture"),
                    live_records=0, next_expiry=None)


def setup(tmp_path):
    manager = RealRemoteLifecycle(tmp_path / "metadata")
    manager.initialize(WAREHOUSE, AUX, ODS, **IDENTITY)
    ods_directory = tmp_path / "ods"
    ods_directory.mkdir()
    sink = FixtureOds(ods_directory)
    hdfs = FixtureHdfs()
    job = dict(run_id="new_job", source="real", input=sink.input, warehouse_root=WAREHOUSE, auxiliary_root=AUX,
               date_from="2026-09-13", date_to="2026-09-13", cutoff=NOW.isoformat())
    coverage = dict(schema_version=1, source="real", **IDENTITY, continuous_from=(NOW - timedelta(days=1)).isoformat(), through=NOW.isoformat(), gaps=[])
    coverage_file = tmp_path / "coverage.json"
    write_json(coverage_file, coverage)
    return manager, job, coverage_file, hdfs, ods_directory, sink


def permit(manager, job, coverage, hdfs, ods_dir, sink, path, **kwargs):
    return manager.issue_permit(job, coverage, None, path, hdfs, ods_dir, sink, now=NOW, **kwargs)


def test_registered_outputs_and_actual_scoped_cleanup_are_required(tmp_path):
    manager, job, coverage, hdfs, ods, sink = setup(tmp_path)
    path = tmp_path / "permit.json"
    with pytest.raises(ValueError, match="Reserve"):
        permit(manager, job, coverage, hdfs, ods, sink, path)
    outputs = manager.reserve_job(job, (NOW - timedelta(days=1)).isoformat(), (NOW - timedelta(days=1)).isoformat(), now=NOW)
    first = permit(manager, job, coverage, hdfs, ods, sink, path)
    assert first["outputs"] == outputs and first["expires_at"] == (NOW + timedelta(minutes=15)).isoformat()
    receipt = json.loads(path.with_suffix(".lifecycle.json").read_bytes())
    assert receipt["backends"]["doris"] == {"scope": "not_initialized", "certified": False}
    assert digest(path.with_suffix(".lifecycle.json").read_bytes()) == first["lifecycle_receipt_sha256"]
    assert not manager.journal.exists()


def test_exact_expired_deletion_retries_without_touching_synthetic_or_live(tmp_path):
    manager, _, _, hdfs, ods, sink = setup(tmp_path)
    old = WAREHOUSE + "/old/raw"
    manager.register(old, "raw", (NOW - timedelta(days=8)).isoformat(), now=NOW - timedelta(days=8))
    live = WAREHOUSE + "/live/aggregate"
    manager.register(live, "aggregate", (NOW - timedelta(days=8)).isoformat(), now=NOW)
    synthetic = "hdfs://snow-control:9000/snow/warehouse/synthetic/never-touch"
    hdfs.files.update({old + "/part", live + "/part", synthetic})
    hdfs.fail_once = True
    with pytest.raises(RuntimeError, match="outage"):
        manager.cleanup(hdfs, ods, sink, now=NOW)
    assert manager.journal.exists() and old in manager._read()[1]["artifacts"]
    receipt = manager.cleanup(hdfs, ods, sink, now=NOW)
    assert receipt["removed"] == [old]
    assert hdfs.exists(live) and hdfs.exists(synthetic) and not hdfs.exists(old)
    assert not manager.journal.exists()


def test_unowned_file_missing_backend_and_forged_receipt_block_permits(tmp_path):
    manager, job, coverage, hdfs, ods, sink = setup(tmp_path)
    manager.reserve_job(job, (NOW - timedelta(days=1)).isoformat(), (NOW - timedelta(days=1)).isoformat(), now=NOW)
    hdfs.files.add(WAREHOUSE + "/unregistered/part")
    target = tmp_path / "blocked.json"
    with pytest.raises(ValueError, match="Unregistered"):
        permit(manager, job, coverage, hdfs, ods, sink, target)
    assert not target.exists()
    hdfs.files.clear()
    # Resolve this empty failed attempt before a distinct registration change.
    manager.cleanup(hdfs, ods, sink, now=NOW)
    manager.register_backend("kafka", "snow.real.fixture.events.v1", "raw", (NOW - timedelta(days=1)).isoformat(), now=NOW)
    with pytest.raises(ValueError, match="Missing actual"):
        permit(manager, job, coverage, hdfs, ods, sink, target)
    adapter = CheckedBackend("kafka")
    adapter.forged = True
    with pytest.raises(ValueError, match="exact registered"):
        permit(manager, job, coverage, hdfs, ods, sink, target, backend_checks={"kafka": adapter})
    adapter.forged = False
    permit(manager, job, coverage, hdfs, ods, sink, target, backend_checks={"kafka": adapter})
    assert adapter.calls == 2 and target.exists()


def test_copy_cannot_renew_retention_or_escape_remote_scope(tmp_path):
    manager, _, _, _, _, _ = setup(tmp_path)
    old = AUX + "/original"
    original = (NOW - timedelta(days=20)).isoformat()
    manager.register(old, "auxiliary", original, now=NOW)
    with pytest.raises(ValueError, match="reset"):
        manager.register(AUX + "/copy", "auxiliary", NOW.isoformat(), copied_from=old, now=NOW)
    manager.register(AUX + "/copy", "auxiliary", original, copied_from=old, now=NOW)
    assert manager._read()[1]["artifacts"][old]["expires_at"] == manager._read()[1]["artifacts"][AUX + "/copy"]["expires_at"]
    for path in (WAREHOUSE, WAREHOUSE + "/../../synthetic", "hdfs://other:9000/snow/warehouse/real/lifecycle_fixture/stolen"):
        with pytest.raises(ValueError):
            manager.register(path, "raw", NOW.isoformat(), now=NOW)


def test_changed_source_identity_and_missing_allocation_cannot_get_permit(tmp_path):
    manager, job, coverage, hdfs, ods, sink = setup(tmp_path)
    manager.reserve_job(job, (NOW - timedelta(days=1)).isoformat(), (NOW - timedelta(days=1)).isoformat(), now=NOW)
    data = json.loads(coverage.read_bytes())
    data["generation"] = "67b920a0-b918-4457-8fbd-802ac414b49b"
    write_json(coverage, data)
    with pytest.raises(ValueError, match="generation"):
        permit(manager, job, coverage, hdfs, ods, sink, tmp_path / "never.json")


def test_bound_ods_collector_must_match_remote_owner_before_cleanup(tmp_path):
    manager, job, coverage, hdfs, ods, sink = setup(tmp_path)
    manager.reserve_job(job, (NOW - timedelta(days=1)).isoformat(), (NOW - timedelta(days=1)).isoformat(), now=NOW)
    state = json.loads((ods / "state.json").read_bytes())
    state["identity"]["collector"]["generation"] = "67b920a0-b918-4457-8fbd-802ac414b49b"
    snapshot = {k: state[k] for k in ("schema_version", "source", "identity", "offsets", "batches", "root", "head_batch_id")}
    state["snapshot_id"] = digest(canonical(snapshot))
    state["input"] = state["root"] + "/snapshots/" + state["snapshot_id"] + "/_snapshot.json"
    write_json(ods / "state.json", state)
    with pytest.raises(ValueError, match="collector generation"):
        permit(manager, job, coverage, hdfs, ods, sink, tmp_path / "never.json")
    assert manager.journal.exists() and not hdfs.deleted
