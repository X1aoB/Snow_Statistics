"""Separate forced-command gateway for registered, lifecycle-approved real jobs."""
import hashlib
import json
import os
import re
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from snow_statistics.real_behavior import real_path, stamp, validate_coverage  # noqa: E402
from snow_statistics.real_lineage import RealCapture, RealJournal, dataset  # noqa: E402
from snow_statistics.real_remote_lifecycle import hive_tables_for  # noqa: E402
from snow_statistics.scheduling import resolve_window  # noqa: E402


def metadata_file(root, name, value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,100}\.json", value):
        raise ValueError("Only a registered metadata filename may be requested")
    return root / "runtime" / "real" / name / value


def build_command(data, root=ROOT, now=None):
    required = {"run_id", "kind", "source", "input", "warehouse_root", "auxiliary_root", "date_from", "date_to",
                "cutoff", "coverage_file", "auxiliary_file", "permit_file"}
    if not required <= set(data) or set(data) - required - {"register_hive"} or data["source"] != "real" or data["kind"] not in {"daily", "behavior"}:
        raise ValueError("Only bounded real event jobs are allowed")
    token = data["run_id"]
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", token):
        raise ValueError("Invalid run ID")
    resolve_window(data["cutoff"], token, data["date_from"], data["date_to"], data["cutoff"])
    if not re.fullmatch(r"hdfs://[A-Za-z0-9.-]+:9000/snow/ods/real/kafka/[a-z0-9-]{1,60}/snapshots/[a-f0-9]{64}/_snapshot.json", data["input"]):
        raise ValueError("Real jobs require a frozen real ODS snapshot")
    real_path(data["warehouse_root"], "warehouse")
    real_path(data["auxiliary_root"], "auxiliary")
    coverage_path = metadata_file(root, "coverage", data["coverage_file"])
    auxiliary_path = metadata_file(root, "auxiliary", data["auxiliary_file"]) if data["auxiliary_file"] else None
    coverage_bytes = coverage_path.read_bytes()
    validate_coverage(json.loads(coverage_bytes), data["cutoff"])
    permit_path = metadata_file(root, "permits", data["permit_file"])
    permit = json.loads(permit_path.read_bytes())
    outputs = dict(daily=data["warehouse_root"] + "/runs/" + token,
                   behavior=data["warehouse_root"] + "/model-runs/" + token + "/behavior",
                   auxiliary=data["auxiliary_root"] + "/" + token)
    if (permit.get("schema_version") != 1 or permit.get("source") != "real" or permit.get("input_snapshot") != data["input"] or
            permit.get("outputs") != outputs or permit.get("coverage_sha256") != hashlib.sha256(coverage_bytes).hexdigest() or
            permit.get("hive_tables", []) != hive_tables_for(data) or
            permit.get("auxiliary_sha256") != (hashlib.sha256(auxiliary_path.read_bytes()).hexdigest() if auxiliary_path else None)):
        raise ValueError("Read permit does not bind these exact inputs and output resources")
    current = now or datetime.now(UTC)
    issued, expiry = stamp(permit["issued_at"]), stamp(permit["expires_at"])
    if not issued <= current < expiry <= issued + timedelta(minutes=15):
        raise ValueError("Real read permit expired or exceeds fifteen minutes")
    receipt_path = permit_path.with_suffix(".lifecycle.json")
    if hashlib.sha256(receipt_path.read_bytes()).hexdigest() != permit["lifecycle_receipt_sha256"]:
        raise ValueError("Lifecycle receipt differs from the read permit")
    filename = token + "." + data["kind"] + ".json"
    output = outputs[data["kind"]]
    command = ["bash", "tools/spark_yarn.sh", "/opt/snow/warehouse/spark/" + ("batch.py" if data["kind"] == "daily" else "real_behavior.py"),
               "--input", data["input"], "--output", data["warehouse_root"] if data["kind"] == "daily" else output,
               "--run-id", token, "--date-from", data["date_from"], "--date-to", data["date_to"], "--cutoff", data["cutoff"],
               "--package-file", "/opt/snow/runtime/real/publication/" + filename]
    if data.get("register_hive", False):
        command += ["--register-hive"]
    if data["kind"] == "daily":
        command += ["--source", "real"]
    else:
        command += ["--coverage-file", "/opt/snow/runtime/real/coverage/" + data["coverage_file"],
                    "--auxiliary-output", outputs["auxiliary"], "--auxiliary-manifest-file", "/opt/snow/runtime/real/auxiliary/" + token + ".json"]
        if auxiliary_path:
            command += ["--auxiliary-input", "/opt/snow/runtime/real/auxiliary/" + data["auxiliary_file"]]
    return command


def execute(data):
    command = build_command(data)
    journal = RealJournal(os.environ["SNOW_REAL_LINEAGE_DB"]) if os.environ.get("SNOW_REAL_LINEAGE_DB") else None
    with RealCapture(journal, "snow_real.compute_" + data["kind"], data["run_id"], [dataset(data["input"])]) as capture:
        subprocess.run(command, cwd=ROOT, check=True, timeout=1100)
        package = json.loads((ROOT / "runtime/real/publication" / (data["run_id"] + "." + data["kind"] + ".json")).read_bytes())
        manifest = package["manifest"]
        if (manifest["source"] != "real" or manifest["engine"] != "Spark 3.5.7" or manifest["master"] != "yarn" or
                not re.fullmatch(r"application_[0-9]+_[0-9]+", manifest["application_id"])):
            raise ValueError("Missing actual real Spark/YARN execution evidence")
        locations = [manifest["output"] + "/ads_daily"] if data["kind"] == "daily" else [manifest["output"] + "/" + name for name in ("session_daily", "retention", "funnel")]
        capture.outputs = [dataset(path) for path in locations]
    print(data["run_id"], flush=True)


if __name__ == "__main__":
    body = sys.stdin.buffer.read(16385)
    if len(body) > 16384:
        raise ValueError("Job request too large")
    execute(json.loads(body))
