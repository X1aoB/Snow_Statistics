import copy
import errno
import io
import json
import os
import signal
import subprocess
import sys
import threading
import time
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from snow_statistics import real_offline_small as small
from snow_statistics.io import digest, write_json
from snow_statistics.publication import canonical

ROOT = Path(__file__).resolve().parents[1]


def config():
    return json.loads((ROOT / "deploy/real-run.example.json").read_bytes())


def host():
    return dict(project_bytes=59 * small.GIB, host_free_bytes=40 * small.GIB, host_available_mib=12000)


def guest(node, containers=None):
    return dict(node=node, available_ram_mib=256, disk_free_mib=512, containers=containers or {})


def item():
    return dict(id="a" * 64, ownership_sha256="b" * 64, memory_current=100, oom=0, oom_kill=0)


class FixtureRunner(small.SmallRunner):
    def __init__(self, tmp_path, *, profile=small.PROFILE):
        self.config, self.config_file, self.root = config(), "runtime/real/config/test.json", tmp_path
        self.profile, self.memory = profile, small.profile_memory(profile)
        self.monitor, self.ready_nodes, self.owned_nodes, self.started_nodes = None, set(), set(), set()
        self.foreign_nodes = set()
        self.identities = {}
        self.inflight, self.attempt, self.calls = set(), "a" * 32, []
        self.state = dict.fromkeys(small.NODES, False)
        self.input = dict(has_pending=True, has_landed=False, has_input=True)
        self.vm = SimpleNamespace(RUNTIME=tmp_path / "runtime/vmware", VMWARE=tmp_path / "vmware-program",
                                  configured_memory=lambda p: self.memory[p.stem],
                                  validate_memory=lambda node, value: value, capacity=lambda _: None)
        self.failure = None

    def vm_state(self):
        return self.state.copy()

    def host(self):
        return host()

    def run(self, command, **kwargs):
        self.calls.append(("run", command, kwargs))

    def vm_command(self, action, node):
        self.calls.append((action, node))
        if self.failure == (action, node):
            raise RuntimeError("Synthetic VM failure")
        if action in {"start", "stop"}:
            self.state[node] = action == "start"

    def probe(self, node, operation="probe", **_):
        self.calls.append((operation, node))
        if self.failure == (operation, node):
            raise ValueError("Synthetic guest/guard failure")
        if operation in {"guard", "input"}:
            return self.input.copy()
        return guest(node)

    def remote(self, node, phase, run_id=None):
        self.calls.append((phase, node))
        if self.failure == (phase, node):
            raise RuntimeError("Synthetic remote phase failure")
        return dict(status="completed", phase=phase)


@pytest.mark.parametrize("phase", small.PHASES)
def test_description_is_explicit_and_non_executing(phase):
    value = small.description(phase, "bounded")
    assert not value["executes"] and not value["new_permit_implied"]
    assert value["memory_mib"] == {"snow-control": 2048, "snow-compute": 1920, "snow-analysis": 768}
    assert value["project_stop_bytes"] == 63.75 * small.GIB
    assert value["project_hard_bytes"] == 64 * small.GIB
    assert value["start_reserve_mib"] == 256


def test_1792_profile_is_explicit_and_does_not_change_default_or_hard_gates():
    default = small.description("start-offline")
    value = small.description("start-offline", profile="real-small-1792")
    assert default["profile"] == "real-small-1920" and default["memory_mib"]["snow-compute"] == 1920
    assert value["memory_mib"] == {"snow-control": 2048, "snow-compute": 1792, "snow-analysis": 768}
    assert value["cold_vm_backing_mib"] == 4608 and value["cold_write_budget_mib"] == 128
    for key in ("start_reserve_mib", "project_stop_bytes", "project_hard_bytes", "host_disk_min_bytes",
                "host_ram_min_mib", "guest_ram_min_mib", "guest_disk_min_mib", "new_permit_implied"):
        assert value[key] == default[key]
    with pytest.raises(ValueError, match="explicit"):
        small.description("start-offline", profile="automatic")


def test_cold_backing_and_write_budget_is_checked_before_configure_or_boot(tmp_path):
    runner = FixtureRunner(tmp_path, profile="real-small-1792")
    last_accepted = small.STOP_BYTES - (4608 + 128) * small.MIB
    runner.host = lambda: host() | {"project_bytes": last_accepted + 1}
    with pytest.raises(RuntimeError, match="128 MiB write budget"):
        runner.start()
    assert runner.calls == [] and not runner.owned_nodes
    allowed = small.check_cold_start(host() | {"project_bytes": last_accepted}, runner.profile)
    assert allowed["projected_bytes"] == small.STOP_BYTES
    with pytest.raises(RuntimeError, match="128 MiB write budget"):
        small.check_cold_start(host() | {"project_bytes": last_accepted}, "real-small-1920")


def test_candidate_vm_commands_use_selected_memory_and_original_start_reserve(tmp_path, monkeypatch):
    runner = FixtureRunner(tmp_path, profile="real-small-1792")
    reservations = []
    monkeypatch.setattr(small, "check_host", lambda sample, reserve=0: reservations.append(reserve))
    small.SmallRunner.vm_command(runner, "configure", "snow-compute")
    small.SmallRunner.vm_command(runner, "start", "snow-compute")
    assert runner.calls[0][1][-2:] == ["--profile", "real-small-1792"]
    assert runner.calls[1][1] == [sys.executable, "tools/vmware_lab.py", "status", "--reserve-mib", "2048"]
    assert runner.calls[1][2] == {"timeout": 90}
    assert runner.calls[2][1] == [str(runner.vm.VMWARE / "vmrun.exe"), "-T", "ws", "start",
                                  str(runner.vm.RUNTIME / "snow-compute/snow-compute.vmx"), "nogui"]
    assert runner.calls[2][2]["vm_control"]
    assert reservations == [1792 + 256]
    assert "--profile real-small-1792" in runner.ssh("snow-compute", "probe")[-1]


def test_vm_soft_stop_bypasses_resource_admission_and_uses_only_direct_control(tmp_path):
    runner = FixtureRunner(tmp_path)
    runner.host = lambda: pytest.fail("Resource admission must never prevent soft stop")
    runner.vm.capacity = lambda _: pytest.fail("No start capacity check during shutdown")
    small.SmallRunner.vm_command(runner, "stop", "snow-analysis")
    _, command, options = runner.calls[-1]
    assert command == [str(runner.vm.VMWARE / "vmrun.exe"), "-T", "ws", "stop",
                       str(runner.vm.RUNTIME / "snow-analysis/snow-analysis.vmx"), "soft"]
    assert options["vm_control"] and not options["monitored"]


def test_actual_vm_memory_mismatch_refuses_before_start(tmp_path):
    runner = FixtureRunner(tmp_path, profile="real-small-1792")
    runner.vm.configured_memory = lambda _: 1920
    with pytest.raises(ValueError, match="memory differs"):
        small.SmallRunner.vm_command(runner, "start", "snow-compute")
    assert not runner.calls


@pytest.mark.parametrize("response", [b"", b"Total running VMs: 1\n", b"Total running VMs: 0\nextra.vmx\n",
                                      b"Total running VMs: 1\ntruncated\n"])
def test_incomplete_vm_inventory_is_not_a_zero_vm_receipt(tmp_path, response):
    runner = FixtureRunner(tmp_path)
    runner.run = lambda *_, **__: response
    with pytest.raises(ValueError, match="inventory"):
        small.SmallRunner.vm_state(runner)


def test_duplicate_inventory_is_refused_and_valid_other_vms_are_not_adopted(tmp_path):
    runner = FixtureRunner(tmp_path)
    other = str(tmp_path / "another.vmx")
    runner.run = lambda *_, **__: f"Total running VMs: 2\n{other}\n{other}\n".encode()
    with pytest.raises(ValueError, match="inventory"):
        small.SmallRunner.vm_state(runner)
    runner.run = lambda *_, **__: f"Total running VMs: 1\n{other}\n".encode()
    assert not any(small.SmallRunner.vm_state(runner).values())


def test_soft_stop_return_is_not_proof_of_shutdown(tmp_path, monkeypatch):
    runner = FixtureRunner(tmp_path)
    runner.owned_nodes = {"snow-analysis"}
    runner.ready_nodes = {"snow-analysis"}
    runner.state["snow-analysis"] = True
    runner.vm_command = lambda *a: None
    clock = iter([0, 31])
    monkeypatch.setattr(small.time, "monotonic", lambda: next(clock))
    with pytest.raises(RuntimeError, match="owned resources remain"):
        runner.stop()


def test_no_input_keeps_ownership_when_stop_readback_fails(tmp_path):
    runner = FixtureRunner(tmp_path)
    runner.input["has_input"] = False
    runner.wait_stopped = lambda _: (_ for _ in ()).throw(ValueError("Synthetic malformed inventory"))
    with pytest.raises(ValueError, match="inventory"):
        runner.start()
    assert runner.owned_nodes == runner.started_nodes == {"snow-analysis"}


def test_failed_capacity_precheck_never_launches_or_marks_uncertain_start(tmp_path):
    runner = FixtureRunner(tmp_path)
    calls = []
    def fail(command, **kwargs):
        calls.append((command, kwargs))
        raise RuntimeError("Synthetic capacity timeout")
    runner.run = fail
    with pytest.raises(RuntimeError, match="capacity"):
        small.SmallRunner.vm_command(runner, "start", "snow-control")
    assert len(calls) == 1 and calls[0][0][2] == "status" and calls[0][1] == {"timeout": 90}
    assert not getattr(runner, "uncertain_starts", set())


def test_failed_vm_start_retains_uncertainty_even_after_soft_stop(tmp_path):
    runner = FixtureRunner(tmp_path)
    def fail_start(command, **kwargs):
        if kwargs.get("vm_control"):
            raise RuntimeError("Synthetic ambiguous VM start")
    runner.run = fail_start
    with pytest.raises(RuntimeError, match="ambiguous"):
        small.SmallRunner.vm_command(runner, "start", "snow-control")
    runner.owned_nodes = runner.started_nodes = {"snow-control"}
    with pytest.raises(RuntimeError, match="uncertain-start:snow-control"):
        runner.stop()
    assert not runner.state["snow-control"]


@pytest.mark.parametrize("failure", ["command_timeout", "active_monitor"])
def test_failed_vmrun_never_kills_the_vm_descendant_tree(tmp_path, monkeypatch, failure):
    runner = FixtureRunner(tmp_path)
    released = threading.Event()
    class Process:
        returncode = None
        terminated = False
        def communicate(self, **_):
            if failure == "command_timeout":
                raise subprocess.TimeoutExpired("synthetic vmrun", 1)
            assert released.wait(5), "Monitor failure should release the owned controller"
            return b"", b""
        def poll(self):
            return self.returncode
        def terminate(self):
            self.terminated, self.returncode = True, -15
            released.set()
        def wait(self, timeout):
            assert self.returncode is not None and timeout <= 10
            return self.returncode
        def kill(self):
            pytest.fail("Synthetic controller terminates gracefully")
    process = Process()
    monkeypatch.setattr(small.subprocess, "Popen", lambda *_, **__: process)
    monkeypatch.setattr(small, "stop_tree", lambda _: pytest.fail("VM descendants must not be tree-killed"))
    def fail_monitor():
        raise RuntimeError("Synthetic active resource failure")
    runner.monitor = SimpleNamespace(check=fail_monitor) if failure == "active_monitor" else None
    with pytest.raises(RuntimeError):
        small.SmallRunner.run(runner, [str(runner.vm.VMWARE / "vmrun.exe"), "-T", "ws", "start"],
                              vm_control=True, timeout=1)
    assert process.terminated and process.returncode == -15


def test_direct_process_mode_cannot_be_used_for_ssh_or_arbitrary_programs(tmp_path, monkeypatch):
    runner = FixtureRunner(tmp_path)
    monkeypatch.setattr(small.subprocess, "Popen", lambda *_, **__: pytest.fail("Invalid control command started"))
    with pytest.raises(ValueError, match="reserved"):
        small.SmallRunner.run(runner, ["ssh", "synthetic"], vm_control=True)


def test_monitor_shutdown_failure_still_stops_every_owned_vm(tmp_path, monkeypatch):
    runner = FixtureRunner(tmp_path)
    runner.failure = ("node-start-offline", "snow-compute")
    class StuckMonitor:
        samples, last = 0, None
        def __init__(self, *_):
            pass
        def start(self):
            pass
        def check(self):
            pass
        def close(self):
            raise RuntimeError("Synthetic monitor did not join")
    monkeypatch.setattr(small, "Monitor", StuckMonitor)
    with pytest.raises(RuntimeError):
        runner.perform("start-offline")
    assert not any(runner.state.values())
    assert {call[1] for call in runner.calls if call[0] == "stop"} == set(small.NODES)
    receipt = small.read_json(tmp_path / "runtime/real/offline-small" / runner.attempt / "controller.json")
    assert receipt["monitor_cleanup_error_type"] == "RuntimeError"
    assert not receipt["cleanup_complete"]  # A live monitor is not successful total cleanup.


@pytest.mark.parametrize("phase", ["land", "stop-offline"])
def test_wrong_selected_profile_does_not_adopt_running_candidate(tmp_path, phase):
    runner = FixtureRunner(tmp_path)
    runner.state = dict.fromkeys(small.NODES, True)
    runner.vm.configured_memory = lambda p: small.PROFILES["real-small-1792"][p.stem]
    with pytest.raises(ValueError, match="memory differs"):
        runner.perform(phase)
    assert not runner.owned_nodes and all(runner.state.values())
    assert not any(call[0] == "stop" for call in runner.calls)


def test_candidate_failure_cleanup_retains_selected_profile(tmp_path):
    runner = FixtureRunner(tmp_path, profile="real-small-1792")
    runner.failure = ("node-start-offline", "snow-compute")
    with pytest.raises(RuntimeError):
        runner.perform("start-offline")
    receipt = small.read_json(tmp_path / "runtime/real/offline-small" / runner.attempt / "controller.json")
    assert receipt["profile"] == "real-small-1792" and receipt["memory_mib"]["snow-compute"] == 1792
    assert receipt["cleanup_complete"] and not any(runner.state.values())


@pytest.mark.parametrize("value", ["all", "sync", "publish-doris", "arbitrary-script", "start-realtime"])
def test_no_realtime_or_arbitrary_phase(value):
    with pytest.raises(ValueError):
        small.description(value)


@pytest.mark.parametrize("change", [dict(project_bytes=small.STOP_BYTES), dict(project_bytes=64 * small.GIB),
                                  dict(host_available_mib=4095), dict(host_free_bytes=35 * small.GIB - 1)])
def test_exact_unrounded_resource_thresholds_fail(change):
    with pytest.raises(RuntimeError):
        small.check_host(host() | change)


def test_reservation_requires_additional_ram_and_capacity():
    assert small.check_host(host(), 256)
    with pytest.raises(RuntimeError):
        small.check_host(host() | {"host_available_mib": 4200}, 256)
    with pytest.raises(RuntimeError):
        small.check_host(host() | {"project_bytes": int(63.7 * small.GIB)}, 512)
    assert small.check_host(host() | {"project_bytes": int(63.7 * small.GIB)}, 256)


@pytest.mark.parametrize("change", [dict(available_ram_mib=127), dict(disk_free_mib=383),
                                  dict(containers={"snow-lab-control-namenode-1": item() | {"oom_kill": 1}})])
def test_guest_resource_failures_are_not_configuration_only(change):
    with pytest.raises(RuntimeError):
        small.check_guest(guest("snow-control") | change, "snow-control")
    assert small.check_guest(guest("snow-control") | change, "snow-control", enforce_resources=False)


def test_unknown_work_is_never_adopted_even_for_stop():
    with pytest.raises(ValueError, match="Unknown"):
        small.check_guest(guest("snow-control", {"business-db": item()}), "snow-control", enforce_resources=False)
    with pytest.raises(ValueError):
        small.check_guest(guest("snow-control", {"snow-spark-yarn": item()}), "snow-control")
    assert small.check_guest(guest("snow-control", {"snow-spark-yarn": item()}), "snow-control", allow_driver=True)


def test_start_requires_all_off_before_any_mutation(tmp_path):
    runner = FixtureRunner(tmp_path)
    runner.state["snow-compute"] = True
    with pytest.raises(ValueError, match="fully stopped"):
        runner.start()
    assert runner.calls == []


def test_analysis_actual_guard_precedes_all_offline_services(tmp_path):
    runner = FixtureRunner(tmp_path)
    assert runner.perform("start-offline")["status"] == "offline_started"
    guard = runner.calls.index(("guard", "snow-analysis"))
    assert guard < runner.calls.index(("start", "snow-control"))
    assert guard < runner.calls.index(("start", "snow-compute"))
    assert all(index > guard for index, call in enumerate(runner.calls) if call[0] == "node-start-offline")
    assert runner.monitor.samples >= 1 and not runner.monitor.thread.is_alive()
    receipt = json.loads((tmp_path / "runtime/real/offline-small" / runner.attempt / "controller.json").read_bytes())
    assert receipt["status"] == "offline_started" and receipt["monitor_samples"] >= 1


def test_empty_input_only_boots_analysis_for_readonly_check(tmp_path):
    runner = FixtureRunner(tmp_path)
    runner.input = dict(has_pending=False, has_landed=False, has_input=False)
    assert runner.perform("start-offline") == dict(status="no_input", offline_services_started=False)
    assert not any(v[0] == "node-start-offline" for v in runner.calls)
    assert [(a, n) for a, n in runner.calls if a in {"start", "stop"}] == [("start", "snow-analysis"), ("stop", "snow-analysis")]


def test_guard_failure_stops_only_attempt_owned_vm_and_preserves_failure_receipt(tmp_path):
    runner = FixtureRunner(tmp_path)
    runner.failure = ("guard", "snow-analysis")
    with pytest.raises(ValueError):
        runner.perform("start-offline")
    assert not runner.state["snow-analysis"]
    assert not any(v[0] == "start" and v[1] != "snow-analysis" for v in runner.calls)
    report = small.read_json(tmp_path / "runtime/real/offline-small" / runner.attempt / "controller.json")
    assert report["status"] == "failed" and report["cleanup_complete"] and not report["data_deleted"]


def test_service_start_failure_stops_all_admitted_nodes(tmp_path):
    runner = FixtureRunner(tmp_path)
    runner.failure = ("node-start-offline", "snow-compute")
    with pytest.raises(RuntimeError):
        runner.perform("start-offline")
    assert not any(runner.state.values())
    assert not runner.monitor.thread.is_alive()


def test_readonly_status_failure_does_not_stop_or_create_receipt(tmp_path):
    runner = FixtureRunner(tmp_path)
    runner.state["snow-control"] = True
    runner.failure = ("probe", "snow-control")
    with pytest.raises(ValueError):
        runner.perform("status")
    assert runner.state["snow-control"]
    assert all(call[0] not in {"stop", "node-stop-offline"} for call in runner.calls)
    assert not (tmp_path / "runtime").exists()


def test_stop_bypasses_host_admission_but_preserves_unknown_work(tmp_path):
    runner = FixtureRunner(tmp_path)
    runner.state = dict.fromkeys(small.NODES, True)
    runner.host = lambda: (_ for _ in ()).throw(RuntimeError("out of capacity"))
    assert runner.perform("stop-offline")["status"] == "offline_stopped"
    assert not any(runner.state.values())


def test_unknown_autostart_work_does_not_trigger_vm_fallback_stop(tmp_path):
    runner = FixtureRunner(tmp_path)
    runner.probe = lambda node, *args, **kw: guest(node, {"foreign-business": item()})
    with pytest.raises(ValueError, match="Unknown"):
        runner.perform("start-offline")
    assert runner.state["snow-analysis"]
    assert not any(call[0] == "stop" for call in runner.calls)
    receipt = small.read_json(tmp_path / "runtime/real/offline-small" / runner.attempt / "controller.json")
    assert receipt["cleanup_complete"] is False


def test_guest_threshold_failure_is_immediate_not_ssh_retry(tmp_path, monkeypatch):
    runner = FixtureRunner(tmp_path)
    runner.probe = lambda node, *args, **kw: guest(node) | {"available_ram_mib": 127}
    monkeypatch.setattr(small.time, "sleep", lambda *a: pytest.fail("resource errors must not retry"))
    with pytest.raises(RuntimeError, match="Guest resource"):
        runner.wait_node("snow-analysis")
    assert "snow-analysis" in runner.ready_nodes


def test_controller_lock_failure_cannot_claim_or_stop_another_attempt(tmp_path, monkeypatch):
    runner = FixtureRunner(tmp_path)
    monkeypatch.setattr(small, "publication_lock", lambda p: (_ for _ in ()).throw(RuntimeError("busy")))
    with pytest.raises(RuntimeError, match="busy"):
        runner.perform("start-offline")
    assert runner.calls == [] and not runner.owned_nodes
    assert not (tmp_path / "runtime").exists()


def test_different_lanes_share_the_same_physical_topology_lock(tmp_path, monkeypatch):
    paths = []
    monkeypatch.setattr(small, "publication_lock", lambda path: paths.append(path) or nullcontext())
    first, second = FixtureRunner(tmp_path), FixtureRunner(tmp_path, profile="real-small-1792")
    first._perform = second._perform = lambda *args: None
    second.config["lane"] = "another-real-epoch"
    first.perform("start-offline")
    second.perform("start-offline")
    assert paths[0] == paths[1] == tmp_path / "runtime/real/offline-small/controller/shared-offline"


@pytest.mark.parametrize("bad_resource", ["host", "guest"])
def test_already_owned_topology_is_closed_on_admission_resource_failure(tmp_path, bad_resource):
    runner = FixtureRunner(tmp_path)
    runner.state = dict.fromkeys(small.NODES, True)
    if bad_resource == "host":
        runner.host = lambda: host() | {"project_bytes": small.STOP_BYTES}
    else:
        probe = runner.probe
        runner.probe = lambda node, operation="probe", **kwargs: (
            guest(node) | {"available_ram_mib": 127} if node == "snow-compute" and operation == "probe"
            else probe(node, operation, **kwargs))
    with pytest.raises(RuntimeError):
        runner.perform("land")
    assert not any(runner.state.values())
    report = small.read_json(tmp_path / "runtime/real/offline-small" / runner.attempt / "controller.json")
    assert report["cleanup_complete"] and report["owned_nodes"] == sorted(small.NODES)


def test_failed_admission_without_ownership_is_not_claimed_as_cleaned(tmp_path):
    runner = FixtureRunner(tmp_path)
    runner.host = lambda: host() | {"project_bytes": small.STOP_BYTES}
    with pytest.raises(RuntimeError):
        runner.perform("start-offline")
    receipt = small.read_json(tmp_path / "runtime/real/offline-small" / runner.attempt / "controller.json")
    assert not receipt["cleanup_complete"] and receipt["cleanup_scope"] == "none_admitted"


def test_same_name_unknown_ownership_is_foreign_not_a_boot_retry(tmp_path):
    runner = FixtureRunner(tmp_path)
    runner.probe = lambda node, *a, **kw: guest(node, {"snow-lab-analysis-datanode-1": item() | {"ownership_sha256": None}})
    with pytest.raises(ValueError):
        runner.perform("start-offline")
    assert "snow-analysis" in runner.foreign_nodes and runner.state["snow-analysis"]
    assert not any(call[0] == "stop" for call in runner.calls)


def test_replacement_under_same_owned_name_is_never_soft_stopped(tmp_path):
    runner = FixtureRunner(tmp_path)
    node, name = "snow-control", "snow-lab-control-namenode-1"
    runner.owned_nodes.add(node)
    runner.remember_objects(node, guest(node, {name: item()}))
    runner.probe = lambda *a, **kw: guest(node, {name: item() | {"id": "c" * 64}})
    with pytest.raises(RuntimeError):
        runner.stop()
    assert node in runner.foreign_nodes and not any(call[0] == "stop" for call in runner.calls)


def container(tmp_path):
    folder = tmp_path / "lab/locks"
    folder.mkdir(parents=True)
    (folder / "images.env").write_text("HADOOP_IMAGE=apache/hadoop@sha256:" + "a" * 64 + "\nSPARK_IMAGE=apache/spark@sha256:" + "b" * 64)
    return dict(id="c" * 64, pid=123, name="/snow-lab-control-namenode-1",
                image="apache/hadoop@sha256:" + "a" * 64, image_id="sha256:" + "d" * 64,
                project="snow-lab-control", service="namenode", working_dir=str(tmp_path / "lab"),
                config_files=",".join(str(tmp_path / "lab" / name) for name in ("compose.control.yaml", "compose.control-scale.yaml")),
                mounts=[dict(Type="bind", Source=str(tmp_path / "lab/generated/hadoop"), Destination="/etc/hadoop", RW=False),
                        dict(Type="volume", Name="snow-lab-control_namenode", Destination="/data/name", RW=True),
                        dict(Type="bind", Source=str(tmp_path / "lab/namenode-entrypoint.sh"), Destination="/snow/namenode-entrypoint.sh", RW=False)])


@pytest.mark.parametrize("mutation", [lambda v: v.update(project="foreign"), lambda v: v.update(service="foreign"),
                                     lambda v: v.update(image="foreign:latest"), lambda v: v.update(working_dir="/business"),
                                     lambda v: v["mounts"][0].update(RW=True),
                                     lambda v: v["mounts"][1].update(Name="business-data"),
                                     lambda v: v.update(config_files="/business/compose.yaml")])
def test_actual_container_projection_rejects_same_name_wrong_owner_or_mount(tmp_path, mutation):
    value = container(tmp_path)
    assert len(small.container_ownership(tmp_path, "snow-control", "snow-lab-control-namenode-1", value)) == 64
    mutation(value)
    with pytest.raises(ValueError):
        small.container_ownership(tmp_path, "snow-control", "snow-lab-control-namenode-1", value)


def hadoop_parent_fixture(tmp_path):
    """De-identified shape of the 2026-09-19 failed analysis DN inspect.

    Creation time/image declaration below are synthetic backend responses: the
    first readback did not query those fields and is not proof they passed.
    """
    value = container(tmp_path)
    value.update(name="/snow-lab-analysis-datanode-1", project="snow-lab-analysis", service="datanode",
                 config_files=str(tmp_path / "lab/compose.analysis.yaml"))
    parent = dict(Type="volume", Name="e" * 64, Source="/var/lib/docker/volumes/" + "e" * 64 + "/_data",
                  Destination="/data", Driver="local", Mode="z", RW=True, Propagation="")
    value["mounts"] = [parent,
                       dict(Type="volume", Name="snow-lab-analysis_datanode", Source="/var/lib/docker/volumes/snow-lab-analysis_datanode/_data",
                            Destination="/data/dn", Driver="local", Mode="rw", RW=True, Propagation=""),
                       dict(Type="bind", Source=str(tmp_path / "lab/generated/hadoop"), Destination="/etc/hadoop", Mode="ro", RW=False, Propagation="rprivate")]
    evidence = dict(image=dict(image_id=value["image_id"], declared_volumes={"/data": {}}),
                    volume=dict(name=parent["Name"], created_at="2026-01-01T00:00:00Z", driver="local",
                                mountpoint=parent["Source"], scope="local", options=None),
                    consumers=[value["id"]])
    return value, parent, evidence


def test_actual_hadoop_shape_requires_declared_parent_and_exact_readback(tmp_path):
    value, _, evidence = hadoop_parent_fixture(tmp_path)
    with pytest.raises(ValueError, match="undeclared"):
        small.container_ownership(tmp_path, "snow-analysis", "snow-lab-analysis-datanode-1", value)
    assert len(small.container_ownership(tmp_path, "snow-analysis", "snow-lab-analysis-datanode-1", value,
                                        parent_probe=lambda *_: evidence)) == 64


@pytest.mark.parametrize("mutation", [lambda v: v["image"].update(declared_volumes={"/data": {}, "/other": {}}),
                                     lambda v: v["image"].update(declared_volumes=None),
                                     lambda v: v["image"].update(image_id="sha256:" + "f" * 64),
                                     lambda v: v.update(consumers=["c" * 64, "d" * 64]),
                                     lambda v: v.update(consumers=[]),
                                     lambda v: v["volume"].update(name="f" * 64),
                                     lambda v: v["volume"].update(created_at="2026-01-01T00:00:00"),
                                     lambda v: v["volume"].update(driver="nfs"),
                                     lambda v: v["volume"].update(scope="global"),
                                     lambda v: v["volume"].update(options={"device": "/private"}),
                                     lambda v: v["volume"].update(mountpoint="/business")])
def test_parent_volume_declaration_reuse_and_identity_cannot_be_forgiven(tmp_path, mutation):
    value, _, evidence = hadoop_parent_fixture(tmp_path)
    mutation(evidence)
    with pytest.raises(ValueError):
        small.container_ownership(tmp_path, "snow-analysis", "snow-lab-analysis-datanode-1", value, parent_probe=lambda *_: evidence)


@pytest.mark.parametrize("mutation", [lambda v: v.update(Type="bind"), lambda v: v.update(Name="named-business-volume"),
                                     lambda v: v.update(Driver="nfs"), lambda v: v.update(RW=False),
                                     lambda v: v.update(Source="/business/private"), lambda v: v.update(Destination="/other")])
def test_parent_mount_does_not_allow_binds_or_other_volumes(tmp_path, mutation):
    value, parent, evidence = hadoop_parent_fixture(tmp_path)
    mutation(parent)
    with pytest.raises(ValueError):
        small.container_ownership(tmp_path, "snow-analysis", "snow-lab-analysis-datanode-1", value, parent_probe=lambda *_: evidence)


def test_parent_readback_is_actual_projected_io_including_stopped_consumers(tmp_path, monkeypatch):
    value, parent, evidence = hadoop_parent_fixture(tmp_path)
    calls = []
    def read(command, **kwargs):
        calls.append(command)
        assert command[:2] == ["sudo", "docker"] and kwargs["timeout"] == 10
        if command[2:4] == ["image", "inspect"]:
            assert command[-1] == value["image_id"]
            assert "Config.Volumes" in command[-2] and "Config.Env" not in command[-2]
            return json.dumps(evidence["image"]).encode()
        if command[2:4] == ["volume", "inspect"]:
            assert command[-1] == parent["Name"]
            return json.dumps(evidence["volume"]).encode()
        assert command[2:] == ["ps", "-a", "--no-trunc", "--filter", "volume=" + parent["Name"], "--format", "{{.ID}}"]
        return (value["id"] + "\n").encode()
    monkeypatch.setattr(small.subprocess, "check_output", read)
    assert small.parent_volume_readback(value, parent) == evidence
    assert len(calls) == 3


def test_legitimate_parent_volume_can_stop_but_recreated_volume_is_rejected(tmp_path):
    value, _, evidence = hadoop_parent_fixture(tmp_path)
    name, node = "snow-lab-analysis-datanode-1", "snow-analysis"
    ownership = small.container_ownership(tmp_path, node, name, value, parent_probe=lambda *_: evidence)
    sample = guest(node, {name: item() | {"id": value["id"], "ownership_sha256": ownership}})
    runner = FixtureRunner(tmp_path)
    runner.owned_nodes.add(node)
    runner.state[node] = True
    runner.remember_objects(node, sample)
    runner.probe = lambda *_: sample
    assert runner.stop()["status"] == "offline_stopped"
    assert ("stop", node) in runner.calls
    runner.calls.clear()
    evidence["volume"]["created_at"] = "2026-01-02T00:00:00Z"
    replacement = small.container_ownership(tmp_path, node, name, value, parent_probe=lambda *_: evidence)
    assert replacement != ownership
    runner.probe = lambda *_: guest(node, {name: item() | {"id": value["id"], "ownership_sha256": replacement}})
    with pytest.raises(RuntimeError):
        runner.stop()
    assert node in runner.foreign_nodes and not any(call[0] == "stop" for call in runner.calls)


def test_cleanup_runs_even_when_no_raw_input_remains(tmp_path):
    runner = FixtureRunner(tmp_path)
    runner.state = dict.fromkeys(small.NODES, True)
    runner.input = dict(has_pending=False, has_landed=False, has_input=False)
    assert runner.perform("cleanup")["status"] == "completed"
    assert ("cleanup", "snow-analysis") in runner.calls


@pytest.mark.parametrize("value", [dict(source="real", reserved=True), dict(same_pair_validated=True),
                                  dict(source="real", computed="daily"), dict(source="real", content_hash="a" * 64)])
def test_frozen_success_without_status_is_only_a_process_completion(tmp_path, value):
    log = tmp_path / "private.log"
    log.write_text("bounded setup output\n" + json.dumps(value) + "\n", encoding="utf-8")
    result = small.phase_result(log, "prepare")
    assert result["status"] == "completed" and result["frozen_process_exit_code"] == 0
    assert "content_hash" not in result and "reserved" not in result


def test_frozen_empty_input_is_preserved_not_reported_as_computed(tmp_path):
    log = tmp_path / "private.log"
    log.write_text(json.dumps(dict(source="real", status="no_computable_input", message="暂无可计算输入")), encoding="utf-8")
    assert small.phase_result(log, "daily") == dict(phase="daily", status="no_computable_input", metrics_fabricated=False)


def expired_lake_copy(tmp_path):
    from snow_statistics.lifecycle import RealLifecycle
    from snow_statistics.real_lake_authority import NAMESPACE
    local = RealLifecycle(tmp_path / NAMESPACE / "fixture-namespace" / "run" / "attempt" / "data")
    local.initialize()
    original = datetime.now(UTC) - timedelta(days=91)
    target = local.register("package.json", "aggregate", original.isoformat(), now=original + timedelta(seconds=1))
    write_json(target, dict(synthetic_test="expired aggregate copy"))
    return target


def test_actual_operator_copy_expiry_is_cleaned_before_any_vm_transport(tmp_path):
    target = expired_lake_copy(tmp_path)
    runner = FixtureRunner(tmp_path)
    vm_command = runner.vm_command
    def command(action, node):
        assert not target.exists()
        return vm_command(action, node)
    runner.vm_command = command
    runner.perform("start-offline")
    assert not target.exists()


def test_unknown_lake_namespace_blocks_new_work_without_deleting_unknown_file(tmp_path):
    from snow_statistics.real_lake_authority import NAMESPACE
    unknown = tmp_path / NAMESPACE / "unregistered.txt"
    unknown.parent.mkdir(parents=True)
    unknown.write_text("synthetic unknown artifact")
    runner = FixtureRunner(tmp_path)
    with pytest.raises(ValueError, match="Unregistered"):
        runner.perform("start-offline")
    assert runner.calls == [] and unknown.exists()
    # Cleanup problems must not disable status or safe stop.
    assert runner.perform("status")["status"] == "observed"
    assert runner.perform("stop-offline")["status"] == "offline_stopped"


def test_actual_node_copy_expiry_precedes_worker_launch(tmp_path, monkeypatch):
    target = expired_lake_copy(tmp_path)
    monkeypatch.setattr(small, "process_identity", lambda pid: dict(pid=pid, start_ticks="1", argv_sha256="a" * 64))
    def launch(*args, **kwargs):
        assert not target.exists()
        raise RuntimeError("synthetic launch boundary reached")
    monkeypatch.setattr(small.subprocess, "Popen", launch)
    with pytest.raises(RuntimeError, match="launch boundary"):
        small.node_execute(config(), "runtime/real/config/test.json", tmp_path, "land", None, "a" * 32,
                           SimpleNamespace(fileno=lambda: 123))
    assert not target.exists()


def test_node_cleanup_failure_blocks_launch_but_cannot_block_cancel(tmp_path, monkeypatch):
    from snow_statistics.real_lake_authority import NAMESPACE
    bad = tmp_path / NAMESPACE / "unknown"
    bad.parent.mkdir(parents=True)
    bad.write_text("synthetic unregistered")
    monkeypatch.setattr(small.subprocess, "Popen", lambda *a, **k: pytest.fail("reader must remain closed"))
    with pytest.raises(ValueError, match="Unregistered"):
        small.node_execute(config(), "runtime/real/config/test.json", tmp_path, "land", None, "a" * 32)
    assert small.node_cancel(tmp_path, config(), "a" * 32)["status"] == "cancelled_before_worker"
    assert bad.exists()


@pytest.mark.skipif(os.name != "posix", reason="Linux SSH SIGHUP and detached process-group boundary")
def test_actual_sighup_closes_detached_frozen_child_and_runs_cleanup(tmp_path):
    (tmp_path / "tools").mkdir()
    child_file, cleanup_file = tmp_path / "child.pid", tmp_path / "cleaned"
    (tmp_path / "tools/real_lab.py").write_text(
        "import os,pathlib,time\npathlib.Path('child.pid').write_text(str(os.getpid()))\ntime.sleep(60)\n")
    code = ("import json,sys,pathlib\nfrom snow_statistics import real_offline_small as s\n"
            "s.driver_cleanup=lambda *a:pathlib.Path(sys.argv[1],'cleaned').write_text('exact cleanup')\n"
            "s.node_execute(json.loads(sys.argv[2]),'runtime/real/config/test.json',pathlib.Path(sys.argv[1]),'land',None,'a'*32)\n")
    parent = subprocess.Popen([sys.executable, "-c", code, str(tmp_path), json.dumps(config())],
                              stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
    child = None
    try:
        parent.stdin.write(b"ping\n")
        parent.stdin.flush()
        deadline = time.monotonic() + 10
        while not child_file.exists() and time.monotonic() < deadline:
            time.sleep(0.05)
        child = int(child_file.read_text())
        os.kill(parent.pid, signal.SIGHUP)
        parent.wait(timeout=20)
        assert cleanup_file.read_text() == "exact cleanup"
        assert small.process_identity(child) is None
    finally:
        small.stop_tree(parent)
        parent.stdin.close()
        if child and small.process_identity(child) is not None:
            os.killpg(child, signal.SIGKILL)


@pytest.mark.skipif(os.name != "posix", reason="Actual Linux worker exit with an open SSH-like stdin pipe")
@pytest.mark.parametrize("exit_code", [0, 7])
def test_actual_worker_exits_with_controller_stdin_still_open(tmp_path, exit_code):
    (tmp_path / "tools").mkdir()
    (tmp_path / "tools/real_lab.py").write_text(
        "import sys,time\ntime.sleep(0.3)\nprint('{\"offline_services\":\"up\"}')\n"
        f"sys.exit({exit_code})\n")
    code = ("import json,sys,pathlib\nfrom snow_statistics import real_offline_small as s\n"
            "s.driver_cleanup=lambda *a:pathlib.Path(sys.argv[1],'cleaned').write_text('exact cleanup')\n"
            "result=s.node_execute(json.loads(sys.argv[2]),'runtime/real/config/test.json',"
            "pathlib.Path(sys.argv[1]),'node-start-offline',None,'a'*32)\nprint(json.dumps(result))\n")
    parent = subprocess.Popen([sys.executable, "-c", code, str(tmp_path), json.dumps(config())],
                              stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              start_new_session=True)
    try:
        parent.stdin.write(b"ping\n")
        parent.stdin.flush()
        # Do not use communicate(): it closes stdin and would hide the actual
        # shutdown defect observed when the Windows SSH controller keeps it open.
        parent.wait(timeout=12)
        assert not parent.stdin.closed
        stdout, stderr = parent.stdout.read(), parent.stderr.read()
        assert b"Fatal Python error" not in stderr and b"_enter_buffered_busy" not in stderr
        assert (tmp_path / "cleaned").read_text() == "exact cleanup"
        if exit_code == 0:
            assert parent.returncode == 0, stderr.decode()
            assert json.loads(stdout)["frozen_process_exit_code"] == 0
        else:
            assert parent.returncode != 0
            assert b"Frozen offline phase failed" in stderr
    finally:
        small.stop_tree(parent)
        parent.stdin.close()
        parent.stdout.close()
        parent.stderr.close()


@pytest.mark.skipif(os.name != "posix", reason="Actual Linux stdin EOF and detached child cleanup")
def test_actual_controller_pipe_eof_stops_detached_child(tmp_path):
    (tmp_path / "tools").mkdir()
    child_file = tmp_path / "child.pid"
    (tmp_path / "tools/real_lab.py").write_text(
        "import os,pathlib,time\npathlib.Path('child.pid').write_text(str(os.getpid()))\ntime.sleep(60)\n")
    code = ("import json,sys,pathlib\nfrom snow_statistics import real_offline_small as s\n"
            "s.driver_cleanup=lambda *a:pathlib.Path(sys.argv[1],'cleaned').write_text('exact cleanup')\n"
            "s.node_execute(json.loads(sys.argv[2]),'runtime/real/config/test.json',"
            "pathlib.Path(sys.argv[1]),'land',None,'a'*32)\n")
    parent = subprocess.Popen([sys.executable, "-c", code, str(tmp_path), json.dumps(config())],
                              stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                              start_new_session=True)
    child = None
    try:
        parent.stdin.write(b"ping\n")
        parent.stdin.flush()
        deadline = time.monotonic() + 10
        while not child_file.exists() and time.monotonic() < deadline:
            time.sleep(0.05)
        child = int(child_file.read_text())
        parent.stdin.close()
        parent.wait(timeout=15)
        stderr = parent.stderr.read()
        assert parent.returncode != 0 and b"Controller heartbeat was lost" in stderr
        assert b"Fatal Python error" not in stderr
        assert (tmp_path / "cleaned").read_text() == "exact cleanup"
        assert small.process_identity(child) is None
    finally:
        small.stop_tree(parent)
        parent.stdin.close()
        parent.stderr.close()
        if child and small.process_identity(child) is not None:
            os.killpg(child, signal.SIGKILL)


def test_cancel_still_runs_when_monitor_rejects_work(tmp_path):
    runner = FixtureRunner(tmp_path)
    runner.remote = small.SmallRunner.remote.__get__(runner)
    runner.ssh = lambda node, operation, **kwargs: [node, operation]
    calls = []
    def run(command, **kwargs):
        calls.append((command, kwargs))
        if command[1] == "execute":
            raise RuntimeError("monitor fault")
    runner.run = run
    with pytest.raises(RuntimeError):
        runner.remote("snow-control", "daily", "new_run")
    assert calls[-1][0] == ["snow-control", "cancel"] and calls[-1][1]["monitored"] is False
    assert runner.inflight == set()


def test_monitor_fault_is_observed_and_thread_is_joined():
    count = [0]
    def changing():
        count[0] += 1
        return host() if count[0] == 1 else host() | {"project_bytes": small.STOP_BYTES}
    monitor = small.Monitor(changing, lambda: [], interval=0.01)
    monitor.start()
    assert monitor.stop.wait(2)
    with pytest.raises(RuntimeError, match="monitor"):
        monitor.check()
    monitor.close()
    assert not monitor.thread.is_alive() and monitor.samples == 1


def test_guest_probe_failure_closes_monitor():
    calls = [0]
    def guests():
        calls[0] += 1
        if calls[0] > 1:
            raise RuntimeError("SSH unavailable")
        return []
    monitor = small.Monitor(host, guests, interval=0.01)
    monitor.start()
    assert monitor.stop.wait(2)
    with pytest.raises(RuntimeError):
        monitor.check()
    monitor.close()


def test_real_paused_guard_binds_collector_ledger_and_actual_stopped_storage(tmp_path, monkeypatch):
    from snow_statistics import landing, real_epoch, real_quiescent
    collector = dict(schema_version=1, source="real", generation="g", instance_id="i")
    write_json(tmp_path / "runtime/real/sync" / config()["lane"] / "source.json", collector)
    calls = []
    class Registry:
        def __init__(self, epoch):
            self.epoch = epoch
        def operation_lock(self):
            return nullcontext()
        def ready(self, identity):
            calls.append(identity)
            return dict(storage={"objects": "same"})
        def complete_recovery(self):
            return [dict(action="paused")]
    monkeypatch.setattr(landing, "collector_identity", lambda v: v)
    monkeypatch.setattr(real_epoch, "Epoch", lambda *args: "exact epoch")
    monkeypatch.setattr(real_quiescent, "DockerStorage", lambda: object())
    monkeypatch.setattr(real_quiescent, "WriterRegistry", Registry)
    def actual(epoch, *, running):
        assert epoch == "exact epoch" and running is False
        return {"objects": "same"}
    monkeypatch.setattr(real_quiescent, "storage", actual)
    result = small.paused_guard(config(), tmp_path)
    assert calls == [collector] and result["actual_engine_objects_stopped"]
    assert result["checkpoint_bytes_rechecked"] is False and result["lifecycle_permit"] is False
    monkeypatch.setattr(Registry, "complete_recovery", lambda s: [dict(action="resume_requested")])
    with pytest.raises(ValueError, match="pause"):
        small.paused_guard(config(), tmp_path)


def test_metadata_scp_has_no_hidden_python_ssh_wrapper(tmp_path):
    runner = FixtureRunner(tmp_path)
    cmd = [sys.executable, "tools/lab_remote.py", "--node", "snow-control", "--download",
           str(tmp_path / "runtime/real/operator/metadata/run/job.json"), "--remote",
           small.REMOTE_ROOT + "/runtime/real/jobs/run.json"]
    result = runner.direct_transfer(cmd)
    assert result[0] == "scp" and "tools/lab_remote.py" not in result
    assert "StrictHostKeyChecking=yes" in result
    assert result[-2].startswith("snow@")
    bad = copy.deepcopy(cmd)
    bad[-1] = small.REMOTE_ROOT + "/runtime/real/../../business"
    with pytest.raises(ValueError):
        runner.direct_transfer(bad)


def test_remote_command_configuration_hash_and_fixed_routing(tmp_path):
    runner = FixtureRunner(tmp_path)
    result = runner.ssh("snow-analysis", "guard")
    assert result[0] == "ssh" and digest(canonical(config())) in result[-1]
    assert "StrictHostKeyChecking=yes" in result
    assert result[-1].startswith("cd /home/snow/Snow_Statistics && exec .venv/bin/python")
    with pytest.raises(ValueError):
        runner.ssh("production", "guard")


def test_node_heartbeat_loss_stops_frozen_process_group(tmp_path, monkeypatch):
    stopped = []
    class Process:
        pid, returncode = 1234, None
        stdout = io.BytesIO(b"synthetic metadata")
        def __init__(self, command, **kwargs):
            assert command[1] == "tools/real_lab.py" and kwargs["start_new_session"]
        def poll(self):
            return self.returncode
    monkeypatch.setattr(small.subprocess, "Popen", Process)
    monkeypatch.setattr(small, "process_identity", lambda pid: dict(pid=pid, start_ticks="1", argv_sha256="a" * 64))
    monkeypatch.setattr(small, "child_session", lambda pid: dict(pid=pid, pgid=pid, sid=pid, start_ticks="1"))
    def stop(identity, process=None):
        stopped.append(identity["pid"])
        return dict(active_members_after_cleanup=0, kill_escalated=False)
    monkeypatch.setattr(small, "stop_session", stop)
    monkeypatch.setattr(small.select, "select", lambda *a: ([123], [], []))
    monkeypatch.setattr(small.os, "read", lambda fd, size: b"")
    with pytest.raises(RuntimeError, match="heartbeat"):
        small.node_execute(config(), "runtime/real/config/test.json", tmp_path, "land", None, "a" * 32,
                           SimpleNamespace(fileno=lambda: 123))
    assert stopped == [1234]
    assert (tmp_path / "runtime/real/offline-small" / ("a" * 32) / "worker.json").exists()


def test_heartbeat_uses_only_ready_bounded_raw_reads_and_complete_frames(monkeypatch):
    clock, ready, calls = [0], [False], []
    chunks = iter([b"pi", b"ng\nping\np", b"ing\n"])
    monkeypatch.setattr(small.time, "monotonic", lambda: clock[0])
    def selected(readers, writers, errors, timeout):
        assert (readers, writers, errors, timeout) == ([123], [], [], 0)
        return ([123] if ready[0] else [], [], [])
    def raw_read(fd, size):
        calls.append((fd, size))
        return next(chunks)
    monkeypatch.setattr(small.select, "select", selected)
    monkeypatch.setattr(small.os, "read", raw_read)
    stream = SimpleNamespace(fileno=lambda: 123, readline=lambda *a: pytest.fail("No buffered reads"))
    heartbeat = small.ControllerHeartbeat(stream)
    heartbeat.check()
    assert calls == []
    ready[0] = True
    clock[0] = 2
    heartbeat.check()
    assert heartbeat.last_ping == 0 and heartbeat.pending == b"pi"
    clock[0] = 3
    heartbeat.check()
    assert heartbeat.last_ping == 3 and heartbeat.pending == b"p"
    clock[0] = 4
    heartbeat.check()
    assert heartbeat.last_ping == 4 and heartbeat.pending == b""
    assert calls == [(123, 64)] * 3


@pytest.mark.parametrize("payload", [b"", b"PING\n", b"ping\r\n", b"other", b"ping\nextra", b"p" * 64])
def test_heartbeat_eof_and_invalid_frames_reject(monkeypatch, payload):
    monkeypatch.setattr(small.select, "select", lambda *a: ([123], [], []))
    monkeypatch.setattr(small.os, "read", lambda *a: payload)
    heartbeat = small.ControllerHeartbeat(SimpleNamespace(fileno=lambda: 123))
    with pytest.raises(RuntimeError, match="heartbeat"):
        heartbeat.check()


@pytest.mark.parametrize("payload", [None, b"p", b"ping\n"])
def test_heartbeat_partial_missing_or_late_input_cannot_extend_deadline(monkeypatch, payload):
    clock = [0]
    monkeypatch.setattr(small.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(small.select, "select", lambda *a: ([123] if payload is not None else [], [], []))
    monkeypatch.setattr(small.os, "read", lambda *a: payload)
    heartbeat = small.ControllerHeartbeat(SimpleNamespace(fileno=lambda: 123))
    clock[0] = 20.01
    with pytest.raises(RuntimeError, match="heartbeat"):
        heartbeat.check()
    assert heartbeat.last_ping == 0


def test_node_cancellation_before_launch_prevents_late_worker(tmp_path, monkeypatch):
    small.node_cancel(tmp_path, config(), "a" * 32)
    monkeypatch.setattr(small.subprocess, "Popen", lambda *a, **k: pytest.fail("must not launch"))
    with pytest.raises(ValueError, match="reused"):
        small.node_execute(config(), "runtime/real/config/test.json", tmp_path, "land", None, "a" * 32)


def test_tampered_cancellation_never_signals(tmp_path, monkeypatch):
    write_json(small.attempt_path(tmp_path, "a" * 32) / "worker.json", dict(config_sha256="wrong", phase="land", run_id=None, process={}))
    monkeypatch.setattr(small.os, "kill", lambda *a: pytest.fail("must not signal"))
    with pytest.raises(ValueError):
        small.node_cancel(tmp_path, config(), "a" * 32)


def test_signal_failure_still_attempts_exact_driver_cleanup(tmp_path, monkeypatch):
    identity = dict(pid=1234, start_ticks="1", argv_sha256="a" * 64)
    write_json(small.attempt_path(tmp_path, "a" * 32) / "worker.json", dict(config_sha256=digest(canonical(config())),
               phase="daily", run_id="run", process=identity))
    monkeypatch.setattr(small, "process_identity", lambda pid: identity)
    monkeypatch.setattr(small.os, "kill", lambda *a: (_ for _ in ()).throw(OSError("synthetic signal failure")))
    calls = []
    monkeypatch.setattr(small, "driver_cleanup", lambda *a: calls.append(a))
    with pytest.raises(OSError):
        small.node_cancel(tmp_path, config(), "a" * 32)
    assert calls == [(tmp_path, "daily", "run")]


def test_process_tree_cleanup_uses_exact_pid_not_image_name(monkeypatch):
    calls = []
    process = SimpleNamespace(pid=1234, poll=lambda: None, wait=lambda **kw: 0)
    monkeypatch.setattr(small.os, "name", "nt")
    monkeypatch.setattr(small.subprocess, "run", lambda command, **kwargs: calls.append(command))
    small.stop_tree(process)
    assert calls == [["taskkill", "/PID", "1234", "/T", "/F"]]


@pytest.mark.skipif(os.name != "nt", reason="Actual Windows descendant termination; POSIX supervisor tested separately")
def test_actual_windows_process_tree_does_not_leave_sleeping_child(tmp_path):
    script = tmp_path / "tree.py"
    pidfile = tmp_path / "pid"
    script.write_text("import subprocess,sys,time,pathlib\n"
                      "p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)'])\n"
                      "pathlib.Path(sys.argv[1]).write_text(str(p.pid))\n"
                      "time.sleep(60)\n")
    parent = subprocess.Popen([sys.executable, str(script), str(pidfile)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        deadline = time.monotonic() + 10
        while not pidfile.exists() and time.monotonic() < deadline:
            time.sleep(0.05)
        child = int(pidfile.read_text())
        small.stop_tree(parent)
        result = subprocess.run(["tasklist", "/FI", "PID eq " + str(child), "/FO", "CSV", "/NH"],
                                capture_output=True, text=True, check=True, timeout=10)
        assert ('"' + str(child) + '"') not in result.stdout
    finally:
        small.stop_tree(parent)


def test_background_monitor_interrupts_running_local_command(tmp_path):
    runner = FixtureRunner(tmp_path)
    runner.run = small.SmallRunner.run.__get__(runner)
    error = RuntimeError("synthetic live threshold crossing")
    runner.monitor = SimpleNamespace(check=lambda: (_ for _ in ()).throw(error))
    began = time.monotonic()
    with pytest.raises(RuntimeError, match="threshold"):
        runner.run([sys.executable, "-c", "import time; time.sleep(60)"], timeout=65)
    assert time.monotonic() - began < 25


@pytest.mark.parametrize("exit_code", [0, 7])
def test_actual_closed_controller_pipe_still_requires_child_exit(tmp_path, exit_code):
    runner = FixtureRunner(tmp_path)
    runner.run = small.SmallRunner.run.__get__(runner)
    command = [sys.executable, "-c", "import os,sys,time; os.close(0); "
               "print('{\"phase\":\"synthetic\"}',flush=True); time.sleep(0.9); "
               f"sys.exit({exit_code})"]
    if exit_code == 0:
        assert json.loads(runner.run(command, capture=True, heartbeat=True, timeout=5)) == {"phase": "synthetic"}
    else:
        with pytest.raises(small.NodeCommandFailure) as caught:
            runner.run(command, capture=True, heartbeat=True, timeout=5)
        assert caught.value.returncode == 7


def test_actual_closed_controller_pipe_does_not_remove_timeout(tmp_path, monkeypatch):
    runner = FixtureRunner(tmp_path)
    runner.run = small.SmallRunner.run.__get__(runner)
    original, processes = small.subprocess.Popen, []
    def launch(*args, **kwargs):
        process = original(*args, **kwargs)
        processes.append(process)
        return process
    monkeypatch.setattr(small.subprocess, "Popen", launch)
    began = time.monotonic()
    with pytest.raises(RuntimeError, match="timed out"):
        runner.run([sys.executable, "-c", "import os,time; os.close(0); time.sleep(30)"],
                   capture=True, heartbeat=True, timeout=1.2)
    assert time.monotonic() - began < 10
    assert processes[0].poll() is not None


def test_actual_closed_controller_pipe_keeps_resource_monitor_active(tmp_path):
    runner = FixtureRunner(tmp_path)
    runner.run = small.SmallRunner.run.__get__(runner)
    samples = []
    def check():
        samples.append(True)
        if len(samples) >= 2:
            raise RuntimeError("synthetic resource stop after pipe closed")
    runner.monitor = SimpleNamespace(check=check)
    with pytest.raises(RuntimeError, match="resource stop after pipe"):
        runner.run([sys.executable, "-c", "import os,time; os.close(0); time.sleep(30)"],
                   capture=True, heartbeat=True, timeout=5)
    assert len(samples) == 2


@pytest.mark.parametrize("failed_operation", ["write", "flush"])
def test_closed_pipe_before_collector_finished_waits_for_actual_result(tmp_path, monkeypatch, failed_operation):
    runner = FixtureRunner(tmp_path)
    runner.run = small.SmallRunner.run.__get__(runner)
    pipe_failed, writes = threading.Event(), []
    class Input:
        def write(self, value):
            writes.append(value)
            if failed_operation == "write":
                pipe_failed.set()
                raise BrokenPipeError(errno.EPIPE, "synthetic closed pipe")
        def flush(self):
            pipe_failed.set()
            raise BrokenPipeError(errno.EPIPE, "synthetic closed pipe")
        def close(self):
            raise BrokenPipeError(errno.EPIPE, "synthetic closed pipe")
    class Process:
        pid, returncode = 1234, None
        def __init__(self, *args, **kwargs):
            self.stdin, self.stdout, self.stderr = Input(), io.BytesIO(b'{"phase":"synthetic"}'), io.BytesIO(b"")
        def wait(self, timeout):
            assert pipe_failed.wait(3), "Controller must reach the exact write/flush race"
            self.returncode = 0
            return 0
        def poll(self):
            return self.returncode
    monkeypatch.setattr(small.subprocess, "Popen", Process)
    assert json.loads(runner.run(["synthetic"], capture=True, heartbeat=True, timeout=5)) == {"phase": "synthetic"}
    assert writes == [b"ping\n"]


@pytest.mark.parametrize("platform,number,expected", [("nt", errno.EINVAL, True), ("posix", errno.EINVAL, False),
                                                     ("nt", errno.EIO, False), ("posix", errno.EIO, False),
                                                     ("nt", errno.EPIPE, True), ("posix", errno.EPIPE, True)])
def test_only_actual_platform_pipe_errors_are_completion_candidates(monkeypatch, platform, number, expected):
    monkeypatch.setattr(small.os, "name", platform)
    assert small.heartbeat_pipe_closed(OSError(number, "synthetic pipe error")) is expected


def test_unknown_heartbeat_io_error_is_not_swallowed(tmp_path, monkeypatch):
    runner = FixtureRunner(tmp_path)
    runner.run = small.SmallRunner.run.__get__(runner)
    wrote = threading.Event()
    class Input:
        def write(self, value):
            wrote.set()
            raise OSError(errno.EIO, "synthetic unexpected heartbeat I/O")
        def close(self):
            pass
    class Process:
        pid, returncode = 1234, None
        def __init__(self, *args, **kwargs):
            self.stdin, self.stdout, self.stderr = Input(), io.BytesIO(b"{}"), io.BytesIO(b"")
        def wait(self, timeout):
            assert wrote.wait(3)
            self.returncode = 0
            return 0
        def poll(self):
            return self.returncode
    monkeypatch.setattr(small.subprocess, "Popen", Process)
    monkeypatch.setattr(small, "stop_tree", lambda p: p.wait(timeout=3))
    with pytest.raises(OSError, match="unexpected heartbeat") as caught:
        runner.run(["synthetic"], capture=True, heartbeat=True, timeout=5)
    assert caught.value.errno == errno.EIO


def test_ssh_prepared_body_is_never_arbitrary_config_command():
    value = config()
    value["command"] = "arbitrary"
    with pytest.raises(ValueError):
        small.checked_config(value)
    value = config() | {"input_origin": "synthetic fixtures", "lane": "fixture-test", "tunnel": None}
    with pytest.raises(ValueError):
        small.checked_config(value)
