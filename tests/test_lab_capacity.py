import importlib
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


def test_small_batch_headroom_cannot_be_spent_as_vm_memory(lab, monkeypatch):
    monkeypatch.setattr(lab, "MAX_PROJECT_BYTES", 1023 * 1024**2)
    monkeypatch.setattr(lab, "run", lambda *args: str((768 + lab.MIN_HOST_AVAILABLE_MIB + 256) * 1024))
    lab.capacity(768)
    with pytest.raises(RuntimeError, match="project gate"):
        lab.capacity(768 + 256)
