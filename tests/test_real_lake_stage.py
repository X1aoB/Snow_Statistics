"""Synthetic existing-container inventories; no Docker, SSH or VM operations."""
import copy
import json

import pytest
from test_real_lake_authority import ATTEMPT, RUN
from test_real_lake_authority import prepared as lake_prepared

from snow_statistics import real_lake_dispatch as dispatch
from snow_statistics import real_lake_stage as stage

prepared = lake_prepared


class Docker:
    def __init__(self, node):
        self.node = node
        self.values = {name: dict(id=name + "-id", identity_sha256=name + "-fixed-hash",
                                 running=name in stage.BASELINE[node], started_at="original", restarts=0)
                       for name in stage.SERVICES[node]}
        self.actions = []
        self.available = 900 * 1024
        self.total = (2000 if node != "snow-analysis" else 730) * 1024
        self.failed = None

    def snapshot(self, node):
        assert node == self.node
        return copy.deepcopy(self.values)

    def memory(self):
        return self.total, self.available

    def change(self, container_id, action):
        self.actions.append((container_id, action))
        if self.failed == (container_id, action):
            raise RuntimeError("synthetic start failure")
        value = next(entry for entry in self.values.values() if entry["id"] == container_id)
        value["running"] = action == "start"
        if action == "start":
            value["started_at"] = "restarted"

    def stop_owned(self, entry):
        current = next(value for value in self.values.values() if value["id"] == entry["id"])
        if current["identity_sha256"] != entry["identity_sha256"]:
            raise ValueError("changed stage container")
        if current["running"]:
            self.change(current["id"], "stop")

    def ready(self, node, service):
        return self.values[service]["running"]

    def hdfs_ready(self):
        assert self.node == "snow-control"
        if not self.values["namenode"]["running"]:
            raise ValueError("synthetic HDFS missing")


@pytest.mark.parametrize("node", list(stage.SERVICES))
def test_existing_ids_switch_in_order_without_touching_any_hdfs_container(prepared, node):
    config, roots, *_ = prepared
    root = roots["operator"] / node
    docker = Docker(node)
    stage.reserve_stage(root, config, RUN, ATTEMPT, node, docker=docker)
    original = copy.deepcopy(docker.values)
    result = stage.switch_stage(root, config, RUN, ATTEMPT, node, docker=docker)
    assert result["phase"] == "yarn"
    assert {key for key, value in docker.values.items() if value["running"]} == stage.YARN[node]
    restored = stage.switch_stage(root, config, RUN, ATTEMPT, node, restore=True, docker=docker)
    assert restored["hdfs_unchanged"] and restored["phase"] == "hive"
    assert {key for key, value in docker.values.items() if value["running"]} == stage.BASELINE[node]
    assert all(not name.startswith(("namenode", "datanode")) for name, _ in docker.actions)
    assert not (root / "runtime/real/lake-stage/owner.json").exists()
    if node == "snow-control":
        assert docker.actions == [("hive-id", "stop"), ("resourcemanager-id", "start"),
                                  ("resourcemanager-id", "stop"), ("hive-id", "start")]
    for key in {"namenode", "datanode"} & set(original):
        assert docker.values[key] == original[key]
    stage.switch_stage(root, config, RUN, ATTEMPT, node, restore=True, docker=docker)


@pytest.mark.parametrize("problem", ["running", "available", "vm", "guest-floor"])
def test_phase_reservation_does_not_adjust_bad_resource_conditions(prepared, problem):
    config, roots, *_ = prepared
    docker = Docker("snow-control")
    if problem == "running":
        docker.values["resourcemanager"]["running"] = True
    elif problem == "available":
        docker.available = 767 * 1024
    elif problem == "guest-floor":
        docker.available = 127 * 1024
    else:
        docker.total = 4096 * 1024
    with pytest.raises(ValueError):
        stage.reserve_stage(roots["operator"], config, RUN, ATTEMPT, "snow-control", docker=docker)
    assert docker.actions == []


def test_explicit_1920_compute_candidate_keeps_original_cgroup_limits(prepared):
    config, roots, *_ = prepared
    docker = Docker("snow-compute")
    docker.total = 1820 * 1024  # Synthetic guest MemTotal after a 1920 MiB allocation.
    stage.reserve_stage(roots["operator"], config, RUN, ATTEMPT, "snow-compute", docker=docker)
    assert stage.SERVICES["snow-compute"] == {"datanode": 384, "nodemanager": 1536}
    assert docker.actions == []  # The helper does not configure/start a VM.


def test_failed_yarn_start_can_restore_only_original_hive_and_preserves_journal(prepared):
    config, roots, *_ = prepared
    root, node = roots["operator"], "snow-control"
    docker = Docker(node)
    original = stage.reserve_stage(root, config, RUN, ATTEMPT, node, docker=docker)
    docker.failed = ("resourcemanager-id", "start")
    with pytest.raises(RuntimeError, match="synthetic"):
        stage.switch_stage(root, config, RUN, ATTEMPT, node, docker=docker)
    assert not docker.values["hive"]["running"]
    docker.failed = None
    stage.switch_stage(root, config, RUN, ATTEMPT, node, restore=True, docker=docker)
    assert docker.values["hive"]["running"]
    assert json.loads(stage._record(root, config, RUN, ATTEMPT, node).read_bytes()) == original


def test_hdfs_drift_does_not_prevent_stopping_our_yarn_but_blocks_restart_claim(prepared):
    config, roots, *_ = prepared
    root, node = roots["operator"], "snow-control"
    docker = Docker(node)
    stage.reserve_stage(root, config, RUN, ATTEMPT, node, docker=docker)
    stage.switch_stage(root, config, RUN, ATTEMPT, node, docker=docker)
    docker.values["namenode"]["running"] = False
    with pytest.raises(ValueError, match="HDFS container changed"):
        stage.switch_stage(root, config, RUN, ATTEMPT, node, restore=True, docker=docker)
    assert not docker.values["resourcemanager"]["running"]
    assert not docker.values["hive"]["running"]
    assert (root / "runtime/real/lake-stage/owner.json").exists()


def test_other_attempt_or_replaced_container_cannot_be_adopted(prepared):
    config, roots, *_ = prepared
    root, node = roots["operator"], "snow-control"
    docker = Docker(node)
    stage.reserve_stage(root, config, RUN, ATTEMPT, node, docker=docker)
    with pytest.raises(ValueError, match="Another lake"):
        stage.reserve_stage(root, config, RUN, "other-attempt", node, docker=docker)
    docker.values["resourcemanager"]["identity_sha256"] = "different-image-mounts-or-limits"
    with pytest.raises(ValueError, match="identity changed"):
        stage.switch_stage(root, config, RUN, ATTEMPT, node, docker=docker)
    assert docker.actions == []
    with pytest.raises(ValueError, match="changed stage"):
        stage.switch_stage(root, config, RUN, ATTEMPT, node, restore=True, docker=docker)


def test_coordinator_attempts_all_node_recovery_and_never_reports_partial_success(prepared):
    config, roots, *_ = prepared
    calls = []
    class Runner(dispatch.LakeRunner):
        def stage(self, node, phase, *unused):
            calls.append((node, phase))
            if node == "snow-compute":
                raise RuntimeError("synthetic timeout")
    runner = Runner(config, "runtime/real/config/test.json", roots["operator"])
    with pytest.raises(RuntimeError, match="recovery incomplete"):
        runner.restore_stages(RUN, ATTEMPT)
    assert calls == [(node, "restore") for node in ("snow-compute", "snow-control", "snow-analysis")]


def test_old_completed_attempt_cannot_stop_later_operator_yarn_work(prepared):
    config, roots, *_ = prepared
    root, node = roots["operator"], "snow-compute"
    docker = Docker(node)
    stage.reserve_stage(root, config, RUN, ATTEMPT, node, docker=docker)
    stage.switch_stage(root, config, RUN, ATTEMPT, node, restore=True, docker=docker)
    docker.values["nodemanager"]["running"] = True
    actions = list(docker.actions)
    with pytest.raises(ValueError, match="Released stage ownership"):
        stage.switch_stage(root, config, RUN, ATTEMPT, node, restore=True, docker=docker)
    assert docker.values["nodemanager"]["running"] and docker.actions == actions


def test_recovery_transport_remains_available_when_capacity_gate_blocks_new_work(prepared, monkeypatch):
    config, roots, *_ = prepared
    runner = dispatch.LakeRunner(config, "runtime/real/config/test.json", roots["operator"])
    calls = []
    monkeypatch.setattr(runner, "run", lambda command, **unused: calls.append(command))
    runner.stage("snow-control", "reserve", RUN, ATTEMPT)
    runner.stage("snow-control", "restore", RUN, ATTEMPT)
    assert calls[0][-2:] == ["--reserve-mib", "256"]
    assert "--reserve-mib" not in calls[1]
    assert calls[1][:4] == [dispatch.sys.executable, "tools/lab_remote.py", "--node", "snow-control"]


def test_lost_reservation_reply_still_recovers_the_attempted_node(prepared):
    config, roots, *_ = prepared
    calls = []
    class Runner(dispatch.LakeRunner):
        def phase(self, node, phase, *unused):
            calls.append((node, phase))
        def stage(self, node, phase, *unused):
            calls.append((node, phase))
            if phase == "reserve":
                raise RuntimeError("synthetic reply lost after remote lease write")
    runner = Runner(config, "runtime/real/config/test.json", roots["operator"])
    with pytest.raises(RuntimeError, match="reply lost"):
        runner.lake(RUN, ATTEMPT)
    assert calls == [("snow-analysis", "reserve"), ("snow-control", "cancel-driver"), ("snow-analysis", "restore")]


def test_absent_attempt_recovery_is_noop_but_unknown_owner_is_not_absent(prepared):
    config, roots, *_ = prepared
    root, node = roots["operator"], "snow-compute"
    docker = Docker(node)
    result = stage.switch_stage(root, config, RUN, ATTEMPT, node, restore=True, docker=docker)
    assert result["stage_unreserved"] and docker.actions == []
    lease, ownership = stage._lease(root, config, RUN, ATTEMPT, node)
    lease.write_text(json.dumps(ownership))
    with pytest.raises(ValueError, match="without its exact reservation"):
        stage.switch_stage(root, config, RUN, ATTEMPT, node, restore=True, docker=docker)
    assert docker.actions == []
