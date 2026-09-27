import hashlib
import importlib.util
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("real_gateway_test", ROOT / "tools/airflow_real_gateway.py")
gateway = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gateway)


def setup(root):
    now = datetime(2026, 9, 14, tzinfo=UTC)
    job = dict(run_id="candidate", kind="behavior", source="real", date_from="2026-09-07", date_to="2026-09-13", cutoff=now.isoformat(),
               input="hdfs://snow-control:9000/snow/ods/real/kafka/test/snapshots/" + "a" * 64 + "/_snapshot.json",
               warehouse_root="hdfs://snow-control:9000/snow/warehouse/real/test", auxiliary_root="hdfs://snow-control:9000/snow/auxiliary/real/test",
               coverage_file="coverage.json", auxiliary_file=None, permit_file="permit.json")
    coverage = dict(schema_version=1, source="real", instance_id="8e98e14c-c0f7-4e94-b020-59d0deef264f",
                    generation="29d191d6-4739-4a62-a2d3-53336314f0a7", continuous_from="2026-09-01T00:00:00Z", through=now.isoformat(), gaps=[])
    folder = root / "runtime/real"
    (folder / "coverage").mkdir(parents=True)
    (folder / "permits").mkdir()
    blob = json.dumps(coverage).encode()
    (folder / "coverage/coverage.json").write_bytes(blob)
    receipt = json.dumps({"scope": "synthetic test backend; no actual cleanup claim"}).encode()
    (folder / "permits/permit.lifecycle.json").write_bytes(receipt)
    permit = dict(schema_version=1, source="real", input_snapshot=job["input"], coverage_sha256=hashlib.sha256(blob).hexdigest(), auxiliary_sha256=None,
                  outputs=dict(daily=job["warehouse_root"] + "/runs/candidate", behavior=job["warehouse_root"] + "/model-runs/candidate/behavior",
                               auxiliary=job["auxiliary_root"] + "/candidate"), issued_at=now.isoformat(), expires_at=(now + timedelta(minutes=10)).isoformat(),
                  lifecycle_receipt_sha256=hashlib.sha256(receipt).hexdigest())
    (folder / "permits/permit.json").write_text(json.dumps(permit))
    return job, now, folder


def test_registered_exact_real_job_builds_shell_free_argv(tmp_path):
    job, now, _ = setup(tmp_path)
    command = gateway.build_command(job, tmp_path, now)
    assert "real_behavior.py" in command[2]
    assert "--auxiliary-output" in command and "--coverage-file" in command
    assert "ops.py" not in " ".join(command)
    job["kind"] = "daily"
    assert gateway.build_command(job, tmp_path, now)[-2:] == ["--source", "real"]


def test_expired_or_modified_permit_inputs_cannot_start_a_job(tmp_path):
    job, now, folder = setup(tmp_path)
    with pytest.raises(ValueError, match="expired"):
        gateway.build_command(job, tmp_path, now + timedelta(minutes=11))
    coverage = json.loads((folder / "coverage/coverage.json").read_bytes())
    coverage["instance_id"] = "b02948ab-a194-49f2-9b73-5d4f74d07b6e"
    (folder / "coverage/coverage.json").write_text(json.dumps(coverage))
    with pytest.raises(ValueError, match="bind"):
        gateway.build_command(job, tmp_path, now)


@pytest.mark.parametrize("field,value", [("source", "synthetic"), ("kind", "operations"),
                          ("coverage_file", "../credentials.json"), ("warehouse_root", "hdfs://snow-control:9000/snow/warehouse/synthetic/test"),
                          ("input", "hdfs://snow-control:9000/snow/ods/real/events.jsonl"), ("run_id", "bad;command")])
def test_namespace_and_job_allowlist(field, value, tmp_path):
    job, now, _ = setup(tmp_path)
    job[field] = value
    with pytest.raises(ValueError):
        gateway.build_command(job, tmp_path, now)
