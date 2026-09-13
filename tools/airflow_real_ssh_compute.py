"""Submit only a pre-registered real job through the separately restricted key."""
import json
import os
import re
import subprocess
from pathlib import Path

name = os.environ["SNOW_REAL_JOB_FILE"]
if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}\.json", name):
    raise ValueError("Expected registered real job filename")
root = Path(__file__).resolve().parents[1]
payload = json.loads((root / "runtime/real/jobs" / name).read_bytes())
payload["kind"] = os.environ["SNOW_JOB_KIND"]
if payload["source"] != "real" or payload["kind"] not in {"daily", "behavior"}:
    raise ValueError("Unexpected real job")
subprocess.run(["ssh", "-T", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
                "-o", "UserKnownHostsFile=/opt/airflow-real-ssh/known_hosts", "-o", "ConnectTimeout=10",
                "-i", "/opt/airflow-real-ssh/id_ed25519", "snow@" + os.environ["SNOW_REAL_COMPUTE_SSH_HOST"]],
               input=json.dumps(payload).encode(), check=True, timeout=1150)
