import importlib
from pathlib import Path

import pytest


@pytest.fixture
def lab(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1] / "tools"))
    return importlib.import_module("vmware_lab")


def test_hive_profile_changes_only_memory_and_preserves_both_data_disks(lab, monkeypatch, tmp_path):
    monkeypatch.setattr(lab, "run", lambda *args: "Total running VMs: 0")
    total = 0
    for node, expected in (("snow-control", 2048), ("snow-compute", 1024), ("snow-analysis", 1536)):
        directory = tmp_path / node
        directory.mkdir()
        disk = directory / "system.vmdk"
        disk.write_bytes(b"synthetic existing disk identity; never recreate on profile changes")
        vmx = directory / (node + ".vmx")
        retained = 'scsi0:0.fileName = "system.vmdk"\nethernet0.connectionType = "nat"\n'
        vmx.write_text('memsize = "6144"\n' + retained, encoding="utf-8")
        lab.configure_memory("fixture-vmrun", vmx, node, "hive-only")
        actual = lab.validate_memory(node, lab.configured_memory(vmx))
        assert actual == expected
        assert vmx.read_text(encoding="utf-8") == f'memsize = "{expected}"\n' + retained
        assert disk.read_bytes().startswith(b"synthetic existing disk identity")
        total += actual
    assert total == 4608  # No more VM backing memory than the reviewed realtime stage.
    assert lab.MAX_PROJECT_BYTES == 64 * 1024**3
    assert lab.MIN_HOST_FREE_BYTES == 35 * 1024**3
    assert lab.MIN_HOST_AVAILABLE_MIB == 4096


@pytest.mark.parametrize("state", ["running", "locked", "suspended"])
def test_hive_profile_cannot_resize_an_active_or_suspended_vm(lab, monkeypatch, tmp_path, state):
    vmx = tmp_path / "snow-analysis.vmx"
    original = 'memsize = "4608"\nscsi0:0.fileName = "system.vmdk"\n'
    vmx.write_text(original, encoding="utf-8")
    monkeypatch.setattr(lab, "run", lambda *args: str(vmx) if state == "running" else "Total running VMs: 0")
    if state == "locked":
        (tmp_path / "snow-analysis.vmx.lck").mkdir()
    elif state == "suspended":
        (tmp_path / "snow-analysis.vmss").write_bytes(b"synthetic suspend marker")
    with pytest.raises(RuntimeError, match="Stop the project VM completely"):
        lab.configure_memory("fixture-vmrun", vmx, "snow-analysis", "hive-only")
    assert vmx.read_text(encoding="utf-8") == original
