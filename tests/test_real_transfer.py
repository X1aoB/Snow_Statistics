"""Synthetic aggregate-only fixtures; no VM or production network operations."""
import copy
import json
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from test_real_publication import packages

from snow_statistics.io import write_json
from snow_statistics.lifecycle import RealLifecycle, timestamp
from snow_statistics.publication import canonical
from snow_statistics.real_lab import Runner, route
from snow_statistics.real_publication import read_real_release, release_real
from snow_statistics.real_transfer import (
    accept,
    cleanup_copies,
    export,
    metadata,
    paths,
    reserve,
    validate_metadata,
    validate_payload,
)

NOW = datetime(2026, 9, 15, tzinfo=UTC)
ROOT = Path(__file__).resolve().parents[1]


def config():
    value = json.loads((ROOT / "deploy/real-run.example.json").read_bytes())
    value["lane"] = "candidate-one"
    return value


def fixture(root):
    value = config()
    daily, behavior = packages()
    for package in (daily, behavior):
        manifest = package["manifest"]
        manifest["input"] = ("hdfs://" + value["nodes"]["snow-control"] + ":9000/snow/ods/real/kafka/"
                             + value["lane"] + "/snapshots/" + "a" * 64 + "/_snapshot.json")
        manifest["input_snapshot"]["offsets"] = {"snow.real.candidate_one.events.v1:0": 2}
    release = release_real(daily, behavior, root / "runtime/real/publication", "transfer01", now=NOW)
    return value, release


def copy_pair(source, destination, value, manifest, *, publish=False, now=NOW):
    relative = paths(value["lane"], "transfer01")
    lifecycle = reserve(destination, value, "transfer01", manifest, now=now)
    shutil.copyfile(source / relative["pair"], lifecycle.path("pair.json.tmp"))
    accept(destination, value, "transfer01", publish=publish, now=now)
    return relative


def test_original_lifetime_is_preserved_across_control_windows_and_analysis_copies(tmp_path):
    control, operator, analysis = (tmp_path / role for role in ("control", "operator", "analysis"))
    value, release = fixture(control)
    manifest = export(control, value, "transfer01", now=NOW)
    relative = copy_pair(control, operator, value, manifest)
    copy_pair(operator, analysis, value, manifest, publish=True)
    assert read_real_release(analysis / relative["published"], NOW) == release
    for root in (control, operator, analysis):
        data = RealLifecycle(root / relative["directory"] / "data")
        entry = data._read()["artifacts"]["pair.json"]
        assert timestamp(entry["expires_at"]) == timestamp(release["expires_at"])
        assert timestamp(entry["original_at"]) == timestamp(manifest["original_at"])
        validate_payload((root / relative["pair"]).read_bytes(), manifest, value, "transfer01", now=NOW)
    assert "anonymous_id" not in (analysis / relative["pair"]).read_text()
    assert "events_realtime" not in (analysis / relative["pair"]).read_text()


def test_transfer_checksum_partial_upload_source_and_extra_private_fields_are_rejected(tmp_path):
    value, release = fixture(tmp_path)
    manifest = export(tmp_path, value, "transfer01", now=NOW)
    payload = canonical(release)
    with pytest.raises(ValueError, match="checksum"):
        validate_payload(payload[:-3], manifest, value, "transfer01", now=NOW)
    for change in (lambda p: p.update(source="synthetic"),
                   lambda p: p["daily"]["daily"][0].update(anonymous_id="fixture-not-a-real-id"),
                   lambda p: p["behavior"].update(raw_events=[])):
        wrong = copy.deepcopy(release)
        change(wrong)
        body = canonical(wrong)
        meta = metadata(wrong, body, value["lane"], "transfer01")
        with pytest.raises(ValueError):
            validate_payload(body, meta, value, "transfer01", now=NOW)
    wrong = manifest | {"lane": "other"}
    with pytest.raises(ValueError):
        reserve(tmp_path, value, "transfer01", wrong, now=NOW)


def test_transfer_never_renews_expiry_and_startup_prunes_all_registered_copies(tmp_path):
    value, release = fixture(tmp_path)
    manifest = export(tmp_path, value, "transfer01", now=NOW)
    destination = tmp_path / "copy"
    relative = copy_pair(tmp_path, destination, value, manifest, publish=True)
    modified = manifest | {"expires_at": (timestamp(manifest["expires_at"]) + timedelta(days=1)).isoformat()}
    with pytest.raises(ValueError, match="renew"):
        reserve(destination, value, "transfer01", modified, now=NOW)
    cutoff = timestamp(release["expires_at"]) + timedelta(seconds=1)
    assert cleanup_copies(destination, now=cutoff) >= 2
    assert not (destination / relative["pair"]).exists()
    with pytest.raises((ValueError, FileNotFoundError)):
        read_real_release(destination / relative["published"], cutoff)
    assert (destination / relative["manifest"]).exists()  # Metadata contains no aggregate values.


def test_unknown_transfer_payload_blocks_cleanup_without_touching_synthetic(tmp_path):
    value, _ = fixture(tmp_path)
    export(tmp_path, value, "transfer01", now=NOW)
    relative = paths(value["lane"], "transfer01")
    unknown = tmp_path / relative["directory"] / "data/unregistered.json"
    unknown.write_text("{}")
    unrelated = tmp_path / "runtime/synthetic/keep.json"
    unrelated.parent.mkdir(parents=True)
    unrelated.write_text("synthetic fixture")
    with pytest.raises(ValueError, match="Unregistered"):
        cleanup_copies(tmp_path, now=NOW)
    assert unrelated.read_text() == "synthetic fixture" and unknown.exists()


@pytest.mark.parametrize("mutation", [
    lambda m: m.update(file="../../business"),
    lambda m: m.update(bytes=5 * 1024**2),
    lambda m: m.update(schema_version=True),
    lambda m: m.update(source="synthetic"),
    lambda m: m.update(input="hdfs://other:9000/private"),
    lambda m: m.update(private_events=[]),
])
def test_transfer_metadata_is_a_strict_source_and_path_allowlist(tmp_path, mutation):
    value, _ = fixture(tmp_path)
    manifest = export(tmp_path, value, "transfer01", now=NOW)
    mutation(manifest)
    with pytest.raises(ValueError):
        validate_metadata(manifest, value, "transfer01", now=NOW)


def test_windows_orchestration_only_copies_the_declared_pair_after_reservation(tmp_path, monkeypatch):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW
    monkeypatch.setattr("snow_statistics.real_transfer.datetime", Clock)
    control, operator, analysis = (tmp_path / role for role in ("control", "operator", "analysis"))
    value, _ = fixture(control)
    calls = []
    class FixtureRunner(Runner):
        def remote(self, node, phase, run_id=None):
            calls.append(("phase", node, phase))
            if phase == "export-release":
                export(control, value, run_id)
            elif phase == "release-directories":
                (analysis / paths(value["lane"], run_id)["directory"]).mkdir(parents=True, exist_ok=True)
            elif phase == "reserve-release":
                relative = paths(value["lane"], run_id)
                reserve(analysis, value, run_id, json.loads((analysis / relative["incoming_manifest"]).read_bytes()))
            elif phase == "import-release":
                accept(analysis, value, run_id, publish=True)
            else:
                pytest.fail("Unexpected remote phase: " + phase)
        def run(self, command, **kwargs):
            calls.append(("scp", command))
            source = control if command[3] == "snow-control" else analysis
            relative = command[-1].removeprefix("/home/snow/Snow_Statistics/")
            if "--download" in command:
                shutil.copyfile(source / relative, command[5])
            else:
                if relative.endswith("pair.json.tmp"):
                    assert (source / Path(relative).parent / "registry.json").exists()
                shutil.copyfile(command[5], source / relative)
    runner = FixtureRunner(value, "runtime/real/config/run.json", operator)
    runner.stage_release("transfer01")
    assert route(value, "publish-doris") == "snow-analysis"
    assert calls[-1] == ("phase", "snow-analysis", "import-release")
    assert read_real_release(analysis / paths(value["lane"], "transfer01")["published"], NOW)["run_id"] == "transfer01"
    assert all("events.jsonl" not in str(call) and "source.json" not in str(call) for call in calls)


def test_failed_transfer_only_requests_its_epoch_stop(tmp_path):
    calls = []
    class FixtureRunner(Runner):
        def remote(self, node, phase, run_id=None):
            calls.append((node, phase, run_id))
            if phase == "export-release":
                raise ValueError("injected validation failure")
    runner = FixtureRunner(config(), "runtime/real/config/run.json", tmp_path)
    with pytest.raises(ValueError):
        runner.stage_release("transfer01")
    assert calls == [("snow-control", "export-release", "transfer01"), ("snow-analysis", "stop-epoch", None)]


def test_expired_received_copy_is_cleaned_before_rejecting_its_metadata(tmp_path):
    value, release = fixture(tmp_path)
    manifest = export(tmp_path, value, "transfer01", now=NOW)
    relative = paths(value["lane"], "transfer01")
    data = RealLifecycle(tmp_path / relative["directory"] / "data")
    write_json(data.path("pair.json.tmp"), release)
    with pytest.raises(ValueError, match="renew"):
        accept(tmp_path, value, "transfer01", now=timestamp(manifest["expires_at"]) + timedelta(seconds=1))
    assert not data.path("pair.json").exists() and not data.path("pair.json.tmp").exists()
