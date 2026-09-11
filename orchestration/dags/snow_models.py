"""A frozen ODS snapshot feeds sequential CDC and behavior model acceptance."""
import json
import os
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

from snow_statistics.model_publication import release_models
from snow_statistics.scheduling import resolve_window


@dag(dag_id="snow_models", start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Hong_Kong"),
     schedule=None, catchup=False, max_active_runs=1,
     default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
     params={"date_from": Param(None, type=["null", "string"], format="date"),
             "date_to": Param(None, type=["null", "string"], format="date"),
             "cutoff": Param(None, type=["null", "string"], format="date-time"),
             "input_snapshot": Param(None, type=["null", "string"])})
def snow_models():
    @task
    def window(**context):
        params = context["params"]
        result = resolve_window(context["data_interval_end"].isoformat(), context["dag_run"].run_id,
                                params["date_from"], params["date_to"], params["cutoff"])
        result["input"] = params["input_snapshot"] or os.environ["SNOW_ODS_PATH"]
        return result

    resolved = window()
    jobs = []
    for kind in ("operations", "behavior"):
        jobs.append(BashOperator(task_id="compute_" + kind,
            bash_command="python /opt/snow/tools/airflow_ssh_compute.py",
            env={"SNOW_JOB_KIND": kind,
                 "SNOW_DATE_FROM": "{{ ti.xcom_pull(task_ids='window')['start'] }}",
                 "SNOW_DATE_TO": "{{ ti.xcom_pull(task_ids='window')['end'] }}",
                 "SNOW_CUTOFF": "{{ ti.xcom_pull(task_ids='window')['cutoff'] }}",
                 "SNOW_ODS_PATH": "{{ ti.xcom_pull(task_ids='window')['input'] }}",
                 "SNOW_RUN_ID": "{{ ti.xcom_pull(task_ids='window')['run_id'] }}-t{{ ti.try_number }}"},
            append_env=True, execution_timeout=timedelta(minutes=20)))

    @task(execution_timeout=timedelta(minutes=2))
    def publish(operations_id, behavior_id, context):
        directory = Path("/opt/snow/runtime/publication")
        operations = json.loads((directory / (operations_id + ".operations.json")).read_bytes())
        behavior = json.loads((directory / (behavior_id + ".behavior.json")).read_bytes())
        result = release_models(operations, behavior, directory, context["run_id"])
        return {"content_hash": result["content_hash"], "run_id": result["run_id"]}

    released = publish(jobs[0].output, jobs[1].output, resolved)
    resolved >> jobs[0] >> jobs[1] >> released


snow_models()
