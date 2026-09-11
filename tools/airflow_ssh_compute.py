"""Submit through a restricted project SSH key; the Airflow image contains no Spark."""
import json
import os
import subprocess
from contextlib import nullcontext

from snow_statistics.lineage import Capture, compute_outputs, input_dataset

keys = ["SNOW_DATE_FROM", "SNOW_DATE_TO", "SNOW_CUTOFF", "SNOW_RUN_ID", "SNOW_SOURCE", "SNOW_ODS_PATH", "SNOW_WAREHOUSE_PATH"]
payload = {key: os.environ[key] for key in keys}
if os.getenv("SNOW_JOB_KIND"):
    payload["SNOW_JOB_KIND"] = os.environ["SNOW_JOB_KIND"]
kind = payload.get("SNOW_JOB_KIND", "daily")
tracking = Capture("snow_models.compute_" + kind, payload["SNOW_RUN_ID"], lambda: input_dataset(payload["SNOW_ODS_PATH"])) if kind in ("operations", "behavior") else nullcontext()
with tracking as capture:
    subprocess.run(["ssh", "-T", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
                    "-o", "UserKnownHostsFile=/opt/airflow-ssh/known_hosts", "-o", "ConnectTimeout=10",
                    "-i", "/opt/airflow-ssh/id_ed25519", "snow@" + os.environ["SNOW_COMPUTE_SSH_HOST"]],
                   input=json.dumps(payload).encode(), check=True)
    if capture:
        capture.outputs(lambda: compute_outputs("/opt/snow/runtime/publication/" + payload["SNOW_RUN_ID"] + "." + kind + ".json", kind))
