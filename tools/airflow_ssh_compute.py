"""Submit through a restricted project SSH key; the Airflow image contains no Spark."""
import json
import os
import subprocess

keys = ["SNOW_DATE_FROM", "SNOW_DATE_TO", "SNOW_CUTOFF", "SNOW_RUN_ID", "SNOW_SOURCE", "SNOW_ODS_PATH", "SNOW_WAREHOUSE_PATH"]
payload = {key: os.environ[key] for key in keys}
if os.getenv("SNOW_JOB_KIND"):
    payload["SNOW_JOB_KIND"] = os.environ["SNOW_JOB_KIND"]
result = subprocess.run(["ssh", "-T", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
                         "-o", "UserKnownHostsFile=/opt/airflow-ssh/known_hosts", "-o", "ConnectTimeout=10",
                         "-i", "/opt/airflow-ssh/id_ed25519", "snow@" + os.environ["SNOW_COMPUTE_SSH_HOST"]],
                        input=json.dumps(payload).encode())
raise SystemExit(result.returncode)
