"""On-demand real events only: registered cleanup -> daily -> behavior -> release.

The external startup coordinator performs cleanup and registers all outputs
before creating the short-lived permit. This DAG never manufactures that permit.
"""
import json
import re
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

from snow_statistics.publication import connect, publish
from snow_statistics.real_publication import release_real, validate_real_pair


@dag(dag_id="snow_real", start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Hong_Kong"),
     schedule=None, catchup=False, max_active_runs=1,
     default_args={"retries": 0}, params={"job_file": Param("", type="string")})
def snow_real():
    @task
    def registered_job(**context):
        name = context["params"]["job_file"]
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}\.json", name):
            raise ValueError("Choose a registered real job; run startup cleanup first")
        data = json.loads((Path("/opt/snow/runtime/real/jobs") / name).read_bytes())
        if data["source"] != "real" or not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", data["run_id"]):
            raise ValueError("Invalid registered real job")
        return {"file": name, "run_id": data["run_id"]}

    selected = registered_job()
    jobs = []
    for kind in ("daily", "behavior"):
        jobs.append(BashOperator(task_id="compute_" + kind,
            bash_command="python /opt/snow/tools/airflow_real_ssh_compute.py",
            env={"SNOW_REAL_JOB_FILE": "{{ ti.xcom_pull(task_ids='registered_job')['file'] }}", "SNOW_JOB_KIND": kind},
            append_env=True, execution_timeout=timedelta(minutes=20)))

    @task(execution_timeout=timedelta(minutes=5))
    def validate_and_publish(context):
        directory = Path("/opt/snow/runtime/real/publication")
        token = context["run_id"]
        daily = json.loads((directory / (token + ".daily.json")).read_bytes())
        behavior = json.loads((directory / (token + ".behavior.json")).read_bytes())
        validate_real_pair(daily, behavior)
        # Both quality gates precede any release. The real DB credential is
        # configured independently; this task does not have a public API role.
        db = connect()
        try:
            receipt = publish(db, daily, directory)
        finally:
            db.close()
        released = release_real(daily, behavior, directory, token)
        return {"source": "real", "run_id": token, "content_hash": released["content_hash"],
                "doris_snapshot": receipt["snapshot_id"]}

    release = validate_and_publish(selected)
    selected >> jobs[0] >> jobs[1] >> release


snow_real()
