import importlib
from pathlib import Path

import pytest


@pytest.fixture
def lab(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1] / "tools"))
    return importlib.import_module("vmware_lab")


@pytest.mark.parametrize("profile,expected_memory,expected_total", [
    ("hive-only", (2048, 1024, 1536), 4608),
    ("real-small-1920", (2048, 1920, 768), 4736),
])
def test_resource_profile_preserves_existing_data_disks(lab, monkeypatch, tmp_path,
                                                       profile, expected_memory, expected_total):
    monkeypatch.setattr(lab, "run", lambda *args: "Total running VMs: 0")
    total = 0
    for node, expected in zip(("snow-control", "snow-compute", "snow-analysis"), expected_memory, strict=True):
        directory = tmp_path / node
        directory.mkdir()
        disk = directory / "system.vmdk"
        disk.write_bytes(b"synthetic existing disk identity; never recreate on profile changes")
        vmx = directory / (node + ".vmx")
        retained = 'scsi0:0.fileName = "system.vmdk"\nethernet0.connectionType = "nat"\n'
        vmx.write_text('memsize = "6144"\n' + retained, encoding="utf-8")
        lab.configure_memory("fixture-vmrun", vmx, node, profile)
        actual = lab.validate_memory(node, lab.configured_memory(vmx))
        assert actual == expected
        assert vmx.read_text(encoding="utf-8") == f'memsize = "{expected}"\n' + retained
        assert disk.read_bytes().startswith(b"synthetic existing disk identity")
        total += actual
    assert total == expected_total
    assert lab.MAX_PROJECT_BYTES == 64 * 1024**3
    assert lab.MIN_HOST_FREE_BYTES == 35 * 1024**3
    assert lab.MIN_HOST_AVAILABLE_MIB == 4096


@pytest.mark.parametrize("state", ["running", "locked", "suspended"])
@pytest.mark.parametrize("profile", ["hive-only", "real-small-1920"])
def test_resource_profile_cannot_resize_active_vm(lab, monkeypatch, tmp_path, state, profile):
    vmx = tmp_path / "snow-analysis.vmx"
    original = 'memsize = "4608"\nscsi0:0.fileName = "system.vmdk"\n'
    vmx.write_text(original, encoding="utf-8")
    monkeypatch.setattr(lab, "run", lambda *args: str(vmx) if state == "running" else "Total running VMs: 0")
    if state == "locked":
        (tmp_path / "snow-analysis.vmx.lck").mkdir()
    elif state == "suspended":
        (tmp_path / "snow-analysis.vmss").write_bytes(b"synthetic suspend marker")
    with pytest.raises(RuntimeError, match="Stop the project VM completely"):
        lab.configure_memory("fixture-vmrun", vmx, "snow-analysis", profile)
    assert vmx.read_text(encoding="utf-8") == original
