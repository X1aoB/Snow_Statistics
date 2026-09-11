"""Airflow 2.10: bounded seven-day correction; explicitly triggered older backfills."""
import os
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

from snow_statistics.scheduling import resolve_window


@dag(dag_id="snow_daily", start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Hong_Kong"),
     schedule=None if os.getenv("SNOW_AIRFLOW_SCHEDULE") == "manual" else "0 3 * * *", catchup=False, max_active_runs=1,
     default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
     params={"date_from": Param(None, type=["null", "string"], format="date"),
             "date_to": Param(None, type=["null", "string"], format="date"),
             "cutoff": Param(None, type=["null", "string"], format="date-time")})
def snow_daily():
    @task
    def window(**context):
        params = context["params"]
        return resolve_window(context["data_interval_end"].isoformat(), context["dag_run"].run_id,
                              params["date_from"], params["date_to"], params["cutoff"])
    resolved = window()
    compute = BashOperator(task_id="compute_and_validate", bash_command="python /opt/snow/tools/airflow_ssh_compute.py",
        env={"SNOW_DATE_FROM": "{{ ti.xcom_pull(task_ids='window')['start'] }}",
             "SNOW_DATE_TO": "{{ ti.xcom_pull(task_ids='window')['end'] }}",
             "SNOW_CUTOFF": "{{ ti.xcom_pull(task_ids='window')['cutoff'] }}",
             "SNOW_RUN_ID": "{{ ti.xcom_pull(task_ids='window')['run_id'] }}-t{{ ti.try_number }}"},
        append_env=True, execution_timeout=timedelta(minutes=20))
    publish = BashOperator(task_id="publish_doris",
        bash_command='python /opt/snow/tools/publish_daily.py "/opt/snow/runtime/publication/$SNOW_RUN_ID.json" --lock-directory /opt/snow/runtime/publication',
        env={"SNOW_RUN_ID": "{{ ti.xcom_pull(task_ids='compute_and_validate') }}"},
        append_env=True, execution_timeout=timedelta(minutes=5))
    resolved >> compute >> publish


snow_daily()
