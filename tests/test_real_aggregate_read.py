"""Bounded private-view leases using only synthetic aggregate fixtures."""
from datetime import timedelta

import pytest
from test_real_hive import fixture
from test_real_publication import NOW
from test_real_remote_lifecycle import FixtureHdfs

from snow_statistics.io import digest, write_json
from snow_statistics.lifecycle import RealLifecycle
from snow_statistics.publication import canonical
from snow_statistics.real_aggregate_read import cached_readable, read_checked
from snow_statistics.real_hive import plan


def setup(tmp_path):
    registry, release, directory = fixture(tmp_path)
    manager = registry.manager
    owner, state = manager._read()
    hdfs = FixtureHdfs()
    for item in plan(release, owner, state["artifacts"], now=NOW).values():
        hdfs.files.add(item["location"] + "/part")
    result = dict(checked_at=NOW.isoformat(), registry_sha256=digest(canonical(state)), next_expiry=None)
    def cleanup():
        return result
    return directory, manager, hdfs, cleanup, result


def test_private_read_requires_both_remote_and_local_gate(tmp_path):
    directory, manager, hdfs, cleanup, _ = setup(tmp_path)
    release, receipt = read_checked(directory, "test", manager, hdfs, cleanup, input_origin="synthetic fixtures", clock=lambda: NOW)
    assert cached_readable(release, receipt, now=NOW + timedelta(seconds=59)) == release
    assert receipt["raw_compute_allowed"] is receipt["writer_restore_allowed"] is False
    with pytest.raises(ValueError, match="expired"):
        cached_readable(release, receipt, now=NOW + timedelta(seconds=60))
    with pytest.raises(ValueError, match="explicit"):
        read_checked(directory, "other", manager, hdfs, cleanup, input_origin="synthetic fixtures", clock=lambda: NOW)


@pytest.mark.parametrize("failure", ["remote_failed", "journal", "missing_hdfs", "registry_change", "stale", "local_expired"])
def test_private_read_failure_never_returns_stale_or_zero_data(tmp_path, failure):
    directory, manager, hdfs, cleanup, result = setup(tmp_path)
    now = NOW
    if failure == "remote_failed":
        def cleanup():
            raise RuntimeError("synthetic backend unavailable")
    elif failure == "journal":
        write_json(manager.journal, {"incomplete": True})
    elif failure == "missing_hdfs":
        hdfs.files.clear()
    elif failure == "registry_change":
        result["registry_sha256"] = "0" * 64
    elif failure == "stale":
        result["checked_at"] = (NOW - timedelta(minutes=11)).isoformat()
    else:
        now = NOW + timedelta(days=91)
        result["checked_at"] = now.isoformat()
    with pytest.raises((ValueError, RuntimeError, FileNotFoundError)):
        read_checked(directory, "test", manager, hdfs, cleanup, input_origin="synthetic fixtures", clock=lambda: now)


def test_bound_cannot_outlive_nearest_remote_expiry_or_change_payload(tmp_path):
    directory, manager, hdfs, cleanup, result = setup(tmp_path)
    result["next_expiry"] = (NOW + timedelta(seconds=7)).isoformat()
    release, receipt = read_checked(directory, "test", manager, hdfs, cleanup, input_origin="synthetic fixtures", clock=lambda: NOW)
    with pytest.raises(ValueError):
        cached_readable(release, receipt, now=NOW + timedelta(seconds=7))
    release["daily"]["daily"][0]["pv"] += 1
    with pytest.raises(ValueError):
        cached_readable(release, receipt, now=NOW)


@pytest.mark.parametrize("failure", ["local_gate", "local_scope", "local_owner", "remote_journal", "remote_scope", "remote_owner"])
def test_existing_lease_closes_immediately_when_registered_gate_changes(tmp_path, failure):
    directory, manager, hdfs, cleanup, _ = setup(tmp_path)
    release, receipt = read_checked(directory, "test", manager, hdfs, cleanup, input_origin="synthetic fixtures", clock=lambda: NOW)
    local = RealLifecycle(directory / "data")
    if failure == "local_gate":
        write_json(local.root / "gate.json", dict(open=False, reason="synthetic_cleanup_failure"))
    elif failure == "local_scope":
        value = local._read()
        value["artifacts"]["new.json"] = dict(kind="aggregate")
        write_json(local.registry, value)
    elif failure == "local_owner":
        from snow_statistics.landing import load
        value = load(local.owner)
        value["instance_id"] = "replaced"
        write_json(local.owner, value)
    elif failure == "remote_journal":
        write_json(manager.journal, dict(source="real", status="cleanup_in_progress"))
    else:
        owner, state = manager._read()
        if failure == "remote_scope":
            state["artifacts"] = {}
            write_json(manager.registry, state)
        else:
            owner["generation"] = "00000000-0000-0000-0000-000000000000"
            write_json(manager.owner, owner)
    with pytest.raises(ValueError, match="closed|changed"):
        cached_readable(release, receipt, now=NOW + timedelta(seconds=1))


def test_lease_is_also_bounded_by_older_local_registered_copy(tmp_path):
    directory, manager, hdfs, cleanup, _ = setup(tmp_path)
    local = RealLifecycle(directory / "data")
    original = NOW + timedelta(seconds=5) - timedelta(days=90)
    path = local.register("old.json", "aggregate", original.isoformat(), now=NOW)
    write_json(path, {"synthetic": True})
    release, receipt = read_checked(directory, "test", manager, hdfs, cleanup, input_origin="synthetic fixtures", clock=lambda: NOW)
    assert receipt["expires_at"] == (NOW + timedelta(seconds=5)).isoformat()
    with pytest.raises(ValueError, match="expired"):
        cached_readable(release, receipt, now=NOW + timedelta(seconds=5))
