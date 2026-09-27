"""Actual loopback synthetic collector; no VM, Kafka or production connectivity."""
import copy
import json
import shutil
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
from urllib.parse import urlsplit

import pytest

from snow_statistics.io import digest, write_json
from snow_statistics.lake_fixture import (
    ADAPTER,
    FixtureSource,
    fixture_config,
    initialize,
    lane_name,
    locations,
    read_fixture,
)
from snow_statistics.landing import pending
from snow_statistics.publication import canonical

NODES = {"snow-control": "192.168.216.131", "snow-compute": "192.168.216.132", "snow-analysis": "192.168.216.133"}
LANE = "fixture-lake-test"


@pytest.fixture(scope="module")
def fixture_root(tmp_path_factory):
    root = tmp_path_factory.mktemp("actual-synthetic-collector")
    result = initialize(root, LANE, NODES)
    assert result["kafka_engine_tested"] is False
    return root


def test_actual_collector_capture_is_bounded_separate_and_explicit(fixture_root):
    paths, config, manifest, rows = read_fixture(fixture_root, LANE)
    assert len(rows) == 28 and config["input_origin"] == "synthetic fixtures"
    assert config["tunnel"] is None and not paths["registry"].exists()
    source = FixtureSource(fixture_root, LANE)
    batch, captured = pending(paths["ods"])
    assert batch and captured["counts"]["events"] == 28 and captured["counts"]["quarantine"] == 0
    assert source.identity()["cluster_id"].startswith("fixture-adapter-")
    assert manifest["transport_adapter"] == ADAPTER and manifest["collector_http_tested"]
    assert not manifest["kafka_engine_tested"]
    assert not (fixture_root / "runtime/real/lifecycle").exists()
    assert not (fixture_root / "runtime/real/epochs").exists()
    assert Path(fixture_root / config["reader_token_file"]).stat().st_size > 16


@pytest.mark.parametrize("lane", ["real-prod-01", "fixture-prod02", "../fixture-lake-aa", "fixture-lake-", "fixture-lake-" + "x" * 11])
def test_lane_cannot_target_production_old_fixtures_or_unbounded_namespace(lane):
    with pytest.raises(ValueError):
        lane_name(lane)


@pytest.mark.parametrize("change", [
    {"input_origin": "real"}, {"transport_node": "snow-control"},
    {"reader_token_file": "runtime/real/secrets/reader.token"},
    {"backend_config_file": "runtime/real/config/backends.json"},
    {"doris_config_file": "runtime/real/config/doris.json"},
    {"collector_url": "https://production.example.invalid"},
])
def test_fixture_rejects_production_connectivity(fixture_root, change):
    _, config, _, _ = read_fixture(fixture_root, LANE)
    with pytest.raises(ValueError):
        fixture_config(config | change, LANE)


def test_retry_cannot_replace_clock_collector_or_old_paths(fixture_root):
    paths = locations(fixture_root, LANE)
    original = (paths["directory"] / "manifest.json").read_bytes()
    with pytest.raises(ValueError, match="fresh"):
        initialize(fixture_root, LANE, NODES)
    assert (paths["directory"] / "manifest.json").read_bytes() == original


def test_fixture_adapter_rejects_partition_or_unbounded_commit(fixture_root):
    source = FixtureSource(fixture_root, LANE)
    for action in (lambda: list(source.read(source.key, 0, 29)), lambda: source.commit({source.key: 29}),
                   lambda: source.committed("snow.real.real_prod_01.events.v1:0")):
        with pytest.raises(ValueError):
            action()
    assert source.committed(source.key) is None


def test_changing_payload_and_its_hash_still_cannot_import_business_events(fixture_root, tmp_path):
    target = tmp_path / "independent"
    shutil.copytree(fixture_root, target)
    paths = locations(target, LANE)
    manifest = json.loads((paths["directory"] / "manifest.json").read_bytes())
    payload_path = paths["directory"] / "data/envelopes.json"
    rows = json.loads(payload_path.read_bytes())
    modified = copy.deepcopy(rows)
    modified[0]["event"]["path"] = "/private-business"
    payload_path.write_bytes(canonical(modified))
    manifest["envelopes_sha256"] = digest(payload_path.read_bytes())
    write_json(paths["directory"] / "manifest.json", manifest)
    with pytest.raises(ValueError, match="newly generated"):
        read_fixture(target, LANE)


class MemoryHdfs:
    """Unit test backend only, never an HDFS integration claim."""
    def __init__(self):
        self.root = "hdfs://192.168.216.131:9000/snow/ods/real/kafka/" + LANE
        self.files = {}
        self.fail = False

    def put_directory(self, relative, files):
        if self.fail:
            raise RuntimeError("unit fixture HDFS unavailable")
        for name, body in files.items():
            path = urlsplit(self.root).path + "/" + relative + "/" + name
            if path in self.files and self.files[path] != body:
                raise ValueError("immutable conflict")
            self.files[path] = body

    def read(self, path):
        return self.files[path]

    def children(self, uri):
        return []


@pytest.fixture
def simulated_hdfs(fixture_root, tmp_path, monkeypatch):
    from snow_statistics import lake_fixture
    target = tmp_path / "independent"
    shutil.copytree(fixture_root, target)
    backend = MemoryHdfs()

    @contextmanager
    def context(config):
        fixture_config(config, LANE)
        yield backend, backend

    monkeypatch.setattr(lake_fixture, "hdfs_context", context)
    return target, backend


def test_hdfs_failure_never_acknowledges_or_issues_permit(simulated_hdfs):
    from snow_statistics.lake_fixture import land_and_reserve
    root, backend = simulated_hdfs
    backend.fail = True
    with pytest.raises(RuntimeError, match="unavailable"):
        land_and_reserve(root, LANE)
    paths = locations(root, LANE)
    assert (paths["ods"] / "pending.json").exists()
    assert not (paths["ods"] / "state.json").exists()
    assert not (paths["directory"] / "adapter-commit.json").exists()
    assert not paths["registry"].exists()


def test_registration_cleanup_and_permit_keep_never_initialized_engines_honest(simulated_hdfs):
    from snow_statistics.lake_fixture import land_and_reserve
    from snow_statistics.real_remote_lifecycle import RealRemoteLifecycle
    root, _ = simulated_hdfs
    result = land_and_reserve(root, LANE)
    assert result["kafka_engine_tested"] is False
    paths, _, fixture, rows = read_fixture(root, LANE)
    assert not (paths["ods"] / "pending.json").exists()
    manager = RealRemoteLifecycle(paths["registry"])
    owner, registry = manager._read()
    assert owner["generation"] == fixture["collector"]["generation"]
    assert len(registry["artifacts"]) == 12
    receipt = json.loads((paths["registry"] / "last-cleanup.json").read_bytes())
    for backend in ("kafka", "doris", "checkpoint", "hive"):
        assert receipt["backends"][backend] == {"scope": "not_initialized", "certified": False}
    coverage = json.loads((root / "runtime/real/coverage" / (LANE + "-r1.json")).read_bytes())
    assert coverage["continuous_from"] == min(row["accepted_at"] for row in rows)
    assert coverage["continuous_from"] > fixture["event_start"]  # No invented historical observation.
    manager.register_backend("kafka", "snow.real.fixture_lake_test.events.v1", "raw", rows[0]["accepted_at"])
    with pytest.raises(ValueError, match="cannot certify"):
        land_and_reserve(root, LANE)


def test_export_rejects_an_older_unrelated_managed_pair(fixture_root, tmp_path, monkeypatch):
    from test_real_publication import NOW, packages

    from snow_statistics import real_publication
    from snow_statistics.lake_fixture import transfer
    from snow_statistics.real_publication import release_real
    root = tmp_path / "other-source"
    shutil.copytree(fixture_root, root)
    daily, behavior = packages()
    release_real(daily, behavior, root / "runtime/real/publication", "old-fixture", now=NOW)
    reader = real_publication.read_real_release
    monkeypatch.setattr(real_publication, "read_real_release", lambda directory: reader(directory, NOW))
    with pytest.raises(ValueError, match="collector"):
        transfer(root, LANE, "export")
    assert not (root / "runtime/real/transfers").exists()


def test_fixture_raw_copy_cannot_remain_readable_after_original_seven_days(fixture_root, tmp_path):
    from snow_statistics.lifecycle import RealLifecycle, timestamp
    root = tmp_path / "expired"
    shutil.copytree(fixture_root, root)
    paths, _, fixture, _ = read_fixture(root, LANE)
    local = RealLifecycle(paths["directory"] / "data")
    local.cleanup(timestamp(fixture["created_at"]) + timedelta(days=7, seconds=1))
    assert not local.path("envelopes.json").exists()
    assert not local.path("collector.sqlite").exists()
    with pytest.raises(ValueError, match="Unregistered"):
        FixtureSource(root, LANE)
