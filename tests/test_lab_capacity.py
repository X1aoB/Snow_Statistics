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
