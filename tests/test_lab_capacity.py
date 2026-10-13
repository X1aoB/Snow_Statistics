import importlib
import json
import runpy
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.fixture
def lab(monkeypatch, tmp_path):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1] / "tools"))
    module = importlib.import_module("vmware_lab")
    monkeypatch.setattr(module, "ROOT", tmp_path)
    monkeypatch.setattr(module, "RUNTIME", tmp_path / "runtime/vmware")
    monkeypatch.setattr(module.shutil, "disk_usage", lambda _: SimpleNamespace(free=100 * 1024**3))
    return module


def test_host_disk_reserve_survives_project_budget_change(lab, monkeypatch):
    monkeypatch.setattr(lab.shutil, "disk_usage", lambda _: SimpleNamespace(free=lab.MIN_HOST_FREE_BYTES-1))
    with pytest.raises(RuntimeError, match="host disk reserve"):
        lab.capacity()


def test_vm_memory_backing_is_reserved_against_project_limit(lab, monkeypatch, tmp_path):
    (tmp_path / "retained.bin").write_bytes(bytes(600 * 1024))
    monkeypatch.setattr(lab, "MAX_PROJECT_BYTES", 1024**2)
    with pytest.raises(RuntimeError, match="project gate"):
        lab.capacity(1)


def test_available_ram_must_cover_reservation_and_host_margin(lab, monkeypatch):
    monkeypatch.setattr(lab, "run", lambda *args: str((1024 + lab.MIN_HOST_AVAILABLE_MIB - 1) * 1024))
    with pytest.raises(RuntimeError, match="Free RAM"):
        lab.capacity(1024)


def test_reduced_memory_is_confined_to_explicit_analysis_option(lab):
    assert lab.validate_memory("snow-analysis", 768) == 768
    for node, memory in (("snow-control", 768), ("snow-compute", 768), ("snow-analysis", 767),
                         ("snow-analysis", 900), ("snow-analysis", 10241)):
        with pytest.raises(RuntimeError, match="reviewed node budget"):
            lab.validate_memory(node, memory)


def test_1792_candidate_is_separate_and_cannot_reconfigure_a_running_vm(lab, tmp_path, monkeypatch):
    assert lab.PROFILES["real-small-1920"]["snow-compute"] == 1920
    assert lab.PROFILES["real-small-1792"] == {"snow-control": 2048, "snow-compute": 1792, "snow-analysis": 768}
    assert lab.validate_memory("snow-compute", 1792) == 1792
    vmx = tmp_path / "snow-compute.vmx"
    original = 'memsize = "1920"\n'
    vmx.write_text(original)
    monkeypatch.setattr(lab, "run", lambda *args: str(vmx))
    with pytest.raises(RuntimeError, match="completely"):
        lab.configure_memory("synthetic-vmrun", vmx, "snow-compute", "real-small-1792")
    assert vmx.read_text() == original


def test_realtime_4096_storage_candidate_is_explicit(lab):
    assert lab.PROFILES["realtime-4096"] == {"snow-analysis": 4096}
    assert lab.validate_memory("snow-analysis", 4096) == 4096


def test_small_batch_headroom_cannot_be_spent_as_vm_memory(lab, monkeypatch):
    monkeypatch.setattr(lab, "MAX_PROJECT_BYTES", 1023 * 1024**2)
    monkeypatch.setattr(lab, "run", lambda *args: str((768 + lab.MIN_HOST_AVAILABLE_MIB + 256) * 1024))
    lab.capacity(768)
    with pytest.raises(RuntimeError, match="project gate"):
        lab.capacity(768 + 256)


def test_expanded_project_budget_still_reserves_full_vm_backing(lab, monkeypatch):
    # Fake measured file sizes: no large files or VMs are created by this test.
    used = [64 * 1024**3]
    file = SimpleNamespace(is_file=lambda: True, stat=lambda: SimpleNamespace(st_size=used[0]),
                           is_relative_to=lambda _: True)
    monkeypatch.setattr(lab, "ROOT", SimpleNamespace(rglob=lambda _: [file]))
    monkeypatch.setattr(lab, "run", lambda *args: str(14000 * 1024))
    assert lab.capacity(4608 + 256)["project_limit_gib"] == 72
    used[0] = lab.MAX_PROJECT_BYTES - (4608 + 256) * 1024**2
    assert lab.capacity(4608 + 256)
    used[0] += 1
    with pytest.raises(RuntimeError, match="project gate"):
        lab.capacity(4608 + 256)


def test_offline_and_vmware_read_the_same_current_resource_policy(lab):
    from snow_statistics import real_offline_small as small

    assert small.LIMIT_BYTES == lab.MAX_PROJECT_BYTES == lab.LIMITS["max_project_bytes"]
    assert small.STOP_BYTES == lab.MAX_PROJECT_BYTES - 256 * 1024**2
    assert small.MIN_HOST_FREE_BYTES == lab.MIN_HOST_FREE_BYTES == 35 * 1024**3
    assert small.MIN_HOST_AVAILABLE_MIB == lab.MIN_HOST_AVAILABLE_MIB == 4096


@pytest.mark.parametrize("existing", [False, True])
def test_control_default_grows_only_a_new_disk(lab, monkeypatch, existing):
    # Only synthetic local files and command captures: no VMware, network or keys.
    directory = lab.RUNTIME / "snow-control"
    directory.mkdir(parents=True)
    (directory / "seed.iso").write_bytes(b"synthetic existing seed")
    disk = directory / "system.vmdk"
    retained = b"synthetic existing 18 GiB disk; default changes must preserve it"
    if existing:
        disk.write_bytes(retained)
    monkeypatch.setitem(sys.modules, "pycdlib", SimpleNamespace())
    monkeypatch.setattr(lab, "NODES", {"snow-control": (2048, 2)})
    monkeypatch.setattr(lab, "capacity", lambda: {})
    monkeypatch.setattr(lab, "download", lambda: lab.RUNTIME / "synthetic-image.vmdk")

    def fake_keypair(path):
        path.write_text("synthetic-key-not-usable")
        path.with_suffix(".pub").write_text("synthetic-public-not-usable")

    monkeypatch.setattr(lab, "keypair", fake_keypair)
    calls = []
    monkeypatch.setattr(lab, "run", lambda *args: calls.append(args))
    lab.prepare()
    if existing:
        assert not calls
        assert disk.read_bytes() == retained
    else:
        assert [args[1] for args in calls] == ["-r", "-x"]
        assert calls[-1][2:] == ("20GB", disk)


def test_compaction_refuses_budget_that_only_covers_old_control_capacity(lab, monkeypatch, capsys):
    # 20 GiB total budget would cover the old 18+1 GiB copy, but not 20+1 GiB.
    directory = lab.RUNTIME / "snow-control"
    directory.mkdir(parents=True)
    (directory / "system.vmdk").write_bytes(b"synthetic thin disk")
    monkeypatch.setattr(lab, "MAX_PROJECT_BYTES", 20 * 1024**3)
    monkeypatch.setattr(lab, "capacity", lambda: {"synthetic": True})
    monkeypatch.setattr(lab, "run", lambda *args: "Total running VMs: 0")
    monkeypatch.setattr(sys, "argv", ["compact_vm_disk.py", "--node", "snow-control"])
    runpy.run_path(str(Path(__file__).resolve().parents[1] / "tools/compact_vm_disk.py"))
    report = json.loads(capsys.readouterr().out)
    assert report["temporary_copy_gib"] == 21
    assert report["powered_off"] is True
    assert report["eligible"] is False


def test_new_disk_defaults_match_registered_capacity_without_maintenance_budget(lab):
    assert lab.DISK_GB == lab.LIMITS["vm_disk_capacity"]["new_disk_default_gib"]
    assert lab.MAX_PROJECT_BYTES == 72 * 1024**3
    assert lab.LIMITS["vm_disk_capacity"]["control_expansion_20261013"]["normal_72_gib_budget_restored"]
