"""Actual temporary-file publication and SQLite journal, using synthetic packages."""
import importlib.util
import json
import os
import re
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = next(parent for parent in Path(__file__).resolve().parents
            if (parent / "src/snow_statistics/real_publication.py").is_file())
if str(ROOT / "tests") not in sys.path:
    sys.path.insert(0, str(ROOT / "tests"))

from test_real_publication import packages  # noqa: E402

from snow_statistics.io import digest, write_json  # noqa: E402
from snow_statistics.lifecycle import RealLifecycle  # noqa: E402
from snow_statistics.publication import canonical, publication_lock  # noqa: E402


@pytest.fixture
def modules(monkeypatch):
    candidate = os.environ.get("SNOW_PUBLISH_CANDIDATE_DIRECTORY")
    if candidate:
        for name in ("real_lineage", "real_publication"):
            qualified = "snow_statistics." + name
            spec = importlib.util.spec_from_file_location(qualified, Path(candidate) / (name + ".py"))
            module = importlib.util.module_from_spec(spec)
            monkeypatch.setitem(sys.modules, qualified, module)
            spec.loader.exec_module(module)
            monkeypatch.setattr(importlib.import_module("snow_statistics"), name, module, raising=False)
    else:
        for name in ("real_lineage", "real_publication"):
            importlib.import_module("snow_statistics." + name)
    return sys.modules["snow_statistics.real_publication"], sys.modules["snow_statistics.real_lineage"]


@pytest.fixture
def prepared(tmp_path, monkeypatch, modules):
    publication, lineage = modules
    daily, behavior = packages()
    # Keep the complete synthetic window in the recent past, independent of CI date.
    shift = datetime.now(UTC).date() - date(2026, 9, 14)
    def moved(value):
        if isinstance(value, dict):
            return {key: moved(item) for key, item in value.items()}
        if isinstance(value, list):
            return [moved(item) for item in value]
        if isinstance(value, str) and re.match(r"^\d{4}-\d{2}-\d{2}(?:T|$)", value):
            return (date.fromisoformat(value[:10]) + shift).isoformat() + value[10:]
        return value
    daily, behavior = moved(daily), moved(behavior)
    run = "fixture-publish"
    for package in (daily, behavior):
        package["manifest"]["run_id"] = run
    directory = tmp_path / "runtime/real/publication"
    monkeypatch.setattr(publication, "CONTROL_PUBLICATION", directory)
    monkeypatch.setattr(publication.socket, "gethostname", lambda: "snow-control")
    origin = daily["manifest"]["date_from"] + "T00:00:00+08:00"
    local = RealLifecycle(tmp_path / "runtime/real/runs" / run / "data")
    local.initialize()
    for kind, package in (("daily", daily), ("behavior", behavior)):
        write_json(local.register(kind + ".json", "aggregate", origin), package)
    journal = lineage.RealJournal(tmp_path / "runtime/real/lineage/events.sqlite")
    return SimpleNamespace(root=tmp_path, publication=publication, lineage=lineage, directory=directory,
                           daily=daily, behavior=behavior, run=run, local=local, journal=journal)


def events(value):
    return [event for _, event, _ in value.journal.pending("fixture")]


def test_existing_model_phase_automatically_records_verified_pair_publish(prepared, monkeypatch):
    from snow_statistics import real_lab
    value = prepared
    monkeypatch.setattr(real_lab, "job_data", lambda *_: ({}, {}))
    result = real_lab.model_phase(SimpleNamespace(root=value.root, config={}), "publish-private", value.run)
    release = value.publication.read_real_release(value.directory)
    assert result == dict(source="real", run_id=value.run, content_hash=release["content_hash"])
    recorded = events(value)
    assert [event["eventType"] for event in recorded] == ["START", "COMPLETE"]
    assert recorded[0]["run"] == recorded[1]["run"] and recorded[0]["outputs"] == []
    assert recorded[1]["job"] == dict(namespace="snow-statistics.real", name="snow_real.publish")
    assert {item["name"] for item in recorded[1]["inputs"]} == {
        "/home/snow/Snow_Statistics/runtime/real/runs/" + value.run + "/data/" + kind + ".json"
        for kind in ("daily", "behavior")}
    relative = "releases/" + digest(canonical(release)) + ".json"
    assert recorded[1]["outputs"] == [dict(namespace="file://snow-control",
        name="/home/snow/Snow_Statistics/runtime/real/publication/data/" + relative)]
    assert (value.directory / "data" / relative).is_file()
    assert not {"daily", "behavior"} & set(json.loads((value.directory / "real-latest.json").read_bytes()))
    payload = json.dumps(recorded)
    assert all(field not in payload for field in ("anonymous_id", "request_id", "retained_d1", "pv", "collector"))
    for event in recorded:
        value.lineage.validate_event(event)


def test_write_failure_records_fail_without_outputs_and_preserves_prior_pointer(prepared, monkeypatch):
    value = prepared
    value.publication.release_real(value.daily, value.behavior, value.directory, value.run)
    original = (value.directory / "real-latest.json").read_bytes()
    actual_write = value.publication.write_json
    def reject_pointer(path, body):
        if path.name == "real-latest.json":
            raise OSError("synthetic sensitive exception must not enter lineage")
        return actual_write(path, body)
    monkeypatch.setattr(value.publication, "write_json", reject_pointer)
    with pytest.raises(OSError, match="synthetic sensitive"):
        value.publication.release_real(value.daily, value.behavior, value.directory, value.run)
    assert (value.directory / "real-latest.json").read_bytes() == original
    recorded = events(value)
    assert [event["eventType"] for event in recorded] == ["START", "COMPLETE", "START", "FAIL"]
    assert recorded[-1]["outputs"] == [] and "synthetic sensitive" not in json.dumps(recorded)


def test_corrupt_pointer_after_write_cannot_emit_complete(prepared, monkeypatch):
    value = prepared
    actual_write = value.publication.write_json
    def corrupt_pointer(path, body):
        result = actual_write(path, body)
        if path.name == "real-latest.json":
            path.write_text("{}", encoding="utf-8")
        return result
    monkeypatch.setattr(value.publication, "write_json", corrupt_pointer)
    with pytest.raises(ValueError, match="pointer"):
        value.publication.release_real(value.daily, value.behavior, value.directory, value.run)
    assert [event["eventType"] for event in events(value)] == ["START", "FAIL"]
    assert events(value)[-1]["outputs"] == []


def test_same_release_retry_gets_new_attempt_without_changing_content_or_expiry(prepared):
    value = prepared
    first = value.publication.release_real(value.daily, value.behavior, value.directory, value.run)
    pointer = (value.directory / "real-latest.json").read_bytes()
    second = value.publication.release_real(value.daily, value.behavior, value.directory, value.run)
    assert second == first and (value.directory / "real-latest.json").read_bytes() == pointer
    recorded = events(value)
    assert [event["eventType"] for event in recorded] == ["START", "COMPLETE", "START", "COMPLETE"]
    assert recorded[0]["run"] != recorded[2]["run"]
    assert recorded[1]["outputs"] == recorded[3]["outputs"]


@pytest.mark.parametrize("state", ["START", "COMPLETE"])
def test_journal_failure_never_returns_success_or_manufactures_complete(prepared, monkeypatch, state):
    value = prepared
    actual_append = value.lineage.RealJournal.append
    def failing_append(self, job, key, current, *args, **kwargs):
        if current == state:
            raise OSError("fixture journal unavailable")
        return actual_append(self, job, key, current, *args, **kwargs)
    monkeypatch.setattr(value.lineage.RealJournal, "append", failing_append)
    with pytest.raises(OSError, match="journal unavailable"):
        value.publication.release_real(value.daily, value.behavior, value.directory, value.run)
    assert [event["eventType"] for event in events(value)] == ([] if state == "START" else ["START"])
    assert (value.directory / "real-latest.json").exists() is (state == "COMPLETE")
    monkeypatch.setattr(value.lineage.RealJournal, "append", actual_append)
    value.publication.release_real(value.daily, value.behavior, value.directory, value.run)
    assert [event["eventType"] for event in events(value)][-2:] == ["START", "COMPLETE"]


@pytest.mark.parametrize("problem", ["different_package", "missing_registration", "wrong_node"])
def test_unproven_input_or_wrong_node_rejected_before_publication(prepared, monkeypatch, problem):
    value = prepared
    if problem == "different_package":
        write_json(value.local.path("daily.json"), {"unregistered": "fixture"})
    elif problem == "missing_registration":
        value.local.registry.unlink()
    else:
        monkeypatch.setattr(value.publication.socket, "gethostname", lambda: "snow-analysis")
    with pytest.raises((ValueError, FileNotFoundError)):
        value.publication.release_real(value.daily, value.behavior, value.directory, value.run)
    assert not (value.directory / "real-latest.json").exists()
    assert not value.journal.path.exists()


def test_release_and_readback_stay_under_same_publisher_lock(prepared, monkeypatch):
    value = prepared
    actual_read = value.publication.read_real_release
    observed = []
    def verify_while_locked(directory, now=None):
        with pytest.raises(OSError):
            with publication_lock(directory):
                raise AssertionError("another publisher acquired the lock")
        observed.append(True)
        return actual_read(directory, now)
    monkeypatch.setattr(value.publication, "read_real_release", verify_while_locked)
    value.publication.release_real(value.daily, value.behavior, value.directory, value.run)
    assert observed == [True]


def test_transfer_copy_does_not_masquerade_as_a_control_publish(prepared):
    value = prepared
    transferred = value.root / "runtime/real/transfers/fixture-publish/run/published"
    released = value.publication.release_real(value.daily, value.behavior, transferred, value.run)
    assert value.publication.read_real_release(transferred) == released
    assert not value.journal.path.exists()


@pytest.mark.parametrize("suffix", ["daily.log", "daily.tmp", "pair.json", "../daily.json", "raw/events.json"])
def test_new_lineage_file_allowlist_never_admits_other_runtime_payloads(modules, suffix):
    _, lineage = modules
    with pytest.raises(ValueError):
        lineage.dataset("file://snow-control/home/snow/Snow_Statistics/runtime/real/runs/fixture/data/" + suffix)
