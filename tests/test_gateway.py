import importlib.util
import io
import json
from pathlib import Path

import pytest

from snow_statistics.model import build
from snow_statistics.simulator import generate

spec = importlib.util.spec_from_file_location("airflow_gateway", Path(__file__).resolve().parents[1] / "tools/airflow_gateway.py")
gateway = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gateway)


@pytest.mark.parametrize("change", [
    {"SNOW_SOURCE": "real"},
    {"SNOW_RUN_ID": "foo;whoami"},
    {"SNOW_ODS_PATH": "file:///etc/passwd"},
    {"SNOW_ODS_PATH": "hdfs://other-host:9000/snow/ods/synthetic/events"},
    {"SNOW_ODS_PATH": "/snow/ods/synthetic/../../real/events"},
    {"SNOW_WAREHOUSE_PATH": "/snow/warehouse-other"},
    {"SNOW_CUTOFF": "2026-01-05"},
])
def test_gateway_rejects_requests_before_spawning(tmp_path, monkeypatch, change):
    (tmp_path / "lab").mkdir()
    (tmp_path / "lab/.env").write_text("CONTROL_IP=192.0.2.1\n")
    data = dict(SNOW_DATE_FROM="2026-01-01", SNOW_DATE_TO="2026-01-04", SNOW_CUTOFF="2026-01-05T00:00:00Z",
                SNOW_RUN_ID="example", SNOW_SOURCE="synthetic", SNOW_ODS_PATH="/snow/ods/synthetic/events",
                SNOW_WAREHOUSE_PATH="/snow/warehouse") | change
    monkeypatch.setattr(gateway, "ROOT", tmp_path)
    monkeypatch.setattr(gateway.sys, "stdin", io.TextIOWrapper(io.BytesIO(json.dumps(data).encode())))
    monkeypatch.setattr(gateway.subprocess, "run", lambda *a, **kw: pytest.fail("Rejected request spawned a process"))
    with pytest.raises(ValueError):
        gateway.main()


def test_gateway_retry_reuses_only_matching_accepted_package(tmp_path, monkeypatch, capsys):
    (tmp_path / "lab").mkdir()
    (tmp_path / "lab/.env").write_text("CONTROL_IP=192.0.2.1\n")
    data = dict(SNOW_DATE_FROM="2026-01-01", SNOW_DATE_TO="2026-01-04", SNOW_CUTOFF="2026-01-05T00:00:00Z",
                SNOW_RUN_ID="example", SNOW_SOURCE="synthetic", SNOW_ODS_PATH="/snow/ods/synthetic/events",
                SNOW_WAREHOUSE_PATH="/snow/warehouse")
    model = build(generate(users=2))
    accepted = {"schema_version": 1, "daily": model["daily"], "manifest": {
        "run_id": "example", "source": "synthetic", "date_from": "2026-01-01", "date_to": "2026-01-04",
        "cutoff": "2026-01-05T00:00:00Z", "input": "/snow/ods/synthetic/events",
        "output": "/snow/warehouse/runs/example", "quality": model["quality"] | {"after_cutoff": 0}}}
    (tmp_path / "runtime/publication").mkdir(parents=True)
    (tmp_path / "runtime/publication/example.json").write_text(json.dumps(accepted))
    monkeypatch.setattr(gateway, "ROOT", tmp_path)
    monkeypatch.setattr(gateway.sys, "stdin", io.TextIOWrapper(io.BytesIO(json.dumps(data).encode())))
    monkeypatch.setattr(gateway.subprocess, "run", lambda *a, **kw: pytest.fail("Accepted request recomputed"))
    gateway.main()
    assert capsys.readouterr().out.strip() == "example"
    data["SNOW_ODS_PATH"] = "/snow/ods/synthetic/other"
    monkeypatch.setattr(gateway.sys, "stdin", io.TextIOWrapper(io.BytesIO(json.dumps(data).encode())))
    with pytest.raises(ValueError, match="different inputs"):
        gateway.main()
