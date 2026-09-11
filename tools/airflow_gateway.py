"""Forced SSH command: validate a bounded synthetic job, invoke the pinned driver.

There is no arbitrary shell, forwarding, command, local file or real-source access.
"""
import json
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from snow_statistics.model_publication import validate_model  # noqa: E402
from snow_statistics.publication import publication_lock, validate  # noqa: E402
from snow_statistics.scheduling import resolve_window  # noqa: E402


def execute(data):
    data = dict(data)
    kind = data.pop("SNOW_JOB_KIND", "daily")
    if kind not in ("daily", "operations", "behavior"):
        raise ValueError("Unknown model job")
    keys = {"SNOW_DATE_FROM", "SNOW_DATE_TO", "SNOW_CUTOFF", "SNOW_RUN_ID", "SNOW_SOURCE", "SNOW_ODS_PATH", "SNOW_WAREHOUSE_PATH"}
    if set(data) != keys or any(not isinstance(v, str) for v in data.values()):
        raise ValueError("Invalid job request")
    token = data["SNOW_RUN_ID"]
    if not re.fullmatch(r"[a-zA-Z0-9_-]{1,100}", token) or data["SNOW_SOURCE"] != "synthetic":
        raise ValueError("Only bounded synthetic lab jobs are allowed")
    resolve_window(data["SNOW_CUTOFF"], token, data["SNOW_DATE_FROM"], data["SNOW_DATE_TO"], data["SNOW_CUTOFF"])
    local_env = dict(line.split("=", 1) for line in (ROOT / "lab/.env").read_text().splitlines() if "=" in line)
    for key, prefix in (("SNOW_ODS_PATH", "/snow/ods/synthetic/"), ("SNOW_WAREHOUSE_PATH", "/snow/warehouse")):
        value = urlsplit(data[key])
        if value.scheme not in ("", "hdfs") or value.netloc not in ("", "snow-control:9000", local_env["CONTROL_IP"] + ":9000"):
            raise ValueError("Only this lab's HDFS may be accessed")
        if not (value.path == prefix.rstrip("/") or value.path.startswith(prefix.rstrip("/") + "/")) or ".." in value.path or value.query or value.fragment or "'" in value.path:
            raise ValueError("Path is outside the laboratory namespace")
    if kind != "daily" and not re.fullmatch(r"/snow/ods/synthetic/kafka/[a-z0-9-]{1,60}/snapshots/[a-f0-9]{64}/_snapshot.json", urlsplit(data["SNOW_ODS_PATH"]).path):
        raise ValueError("Model jobs require a frozen ODS snapshot")
    filename = token + ("" if kind == "daily" else "." + kind) + ".json"
    package = ROOT / "runtime/publication" / filename
    output = data["SNOW_WAREHOUSE_PATH"].rstrip("/") + ("/runs/" + token if kind == "daily" else "/model-runs/" + token + "/" + kind)
    with publication_lock(ROOT / "runtime/compute-gateway"):
        if package.exists():
            value = json.loads(package.read_text())
            previous = validate(value)[0] if kind == "daily" else validate_model(value, kind)
            if previous.get("input") != data["SNOW_ODS_PATH"] or previous["output"] != output:
                raise ValueError("Run ID already belongs to different inputs/outputs")
            if (previous["date_from"], previous["date_to"], datetime.fromisoformat(previous["cutoff"].replace("Z", "+00:00"))) != (
                data["SNOW_DATE_FROM"], data["SNOW_DATE_TO"], datetime.fromisoformat(data["SNOW_CUTOFF"].replace("Z", "+00:00"))):
                raise ValueError("Run ID already belongs to a different window")
        else:
            command = ["bash", "tools/spark_yarn.sh", "/opt/snow/warehouse/spark/" + {"daily": "batch", "operations": "ops", "behavior": "behavior"}[kind] + ".py",
                       "--input", data["SNOW_ODS_PATH"], "--output", data["SNOW_WAREHOUSE_PATH"] if kind == "daily" else output,
                       "--run-id", token, "--date-from", data["SNOW_DATE_FROM"],
                       "--as-of" if kind == "operations" else "--date-to", data["SNOW_DATE_TO"],
                       "--cutoff", data["SNOW_CUTOFF"], "--register-hive", "--package-file", "/opt/snow/runtime/publication/" + filename]
            if kind == "daily":
                command.extend(["--source", "synthetic"])
            subprocess.run(command, cwd=ROOT, check=True, timeout=1100)
    print(token, flush=True)


def main():
    execute(json.loads(sys.stdin.buffer.read(16385)))


if __name__ == "__main__":
    main()
