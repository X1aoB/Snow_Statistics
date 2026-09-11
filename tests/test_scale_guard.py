import importlib
import json
import subprocess
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest


@pytest.fixture
def guard(monkeypatch, tmp_path):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1] / "tools"))
    module = importlib.import_module("run_scale_guarded")
    monkeypatch.setattr(module, "ROOT", tmp_path)
    return module


def test_capacity_refusal_persists_receipt_without_starting_job(guard, monkeypatch, tmp_path):
    monkeypatch.setattr(sys, "argv", ["guard", "--events", "1000000", "--attempt", "refused-01"])
    monkeypatch.setattr(guard, "capacity", Mock(side_effect=RuntimeError("RAM reserve failed")))
    launch = Mock(side_effect=AssertionError("Must not launch after refusal"))
    monkeypatch.setattr(guard.subprocess, "Popen", launch)
    with pytest.raises(SystemExit, match="no job started"):
        guard.main()
    receipt = json.loads((tmp_path / "runtime/scale/refused-01-monitor.json").read_bytes())
    assert receipt["job_started"] is False and receipt["phase"] == "preflight_rejected"
    launch.assert_not_called()


def test_failed_driver_stop_does_not_skip_remaining_project_nodes(guard, monkeypatch, tmp_path):
    commands = []

    def execute(command, **kwargs):
        assert kwargs["timeout"] <= 60
        commands.append(command)
        if len(commands) == 1:
            raise subprocess.CalledProcessError(1, command)

    monkeypatch.setattr(guard.subprocess, "run", execute)
    child = Mock()
    child.wait.side_effect = [subprocess.TimeoutExpired("ssh", 45), 1]
    errors = guard.stop_project(child, tmp_path / "stop-scale-job.sh")
    assert errors == [dict(step="stop_driver", error="CalledProcessError")]
    child.terminate.assert_called_once()
    vm_stops = [command[-1] for command in commands if command[1] == "tools/vmware_lab.py"]
    assert vm_stops == ["snow-compute", "snow-analysis", "snow-control"]
    assert len(commands) == 7
