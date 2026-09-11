"""Airflow 2.10: bounded seven-day correction; explicitly triggered older backfills."""
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.bash import BashOperator


@dag(dag_id="snow_daily", start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Hong_Kong"),
     schedule="0 3 * * *", catchup=False, max_active_runs=1,
     default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
     params={"date_from": Param(None, type=["null", "string"], format="date"),
             "date_to": Param(None, type=["null", "string"], format="date")})
def snow_daily():
    @task
    def window(**context):
        end = context["data_interval_end"].in_timezone("Asia/Hong_Kong").subtract(days=1).date()
        params = context["params"]
        start = pendulum.parse(params["date_from"]).date() if params["date_from"] else end - timedelta(days=6)
        end = pendulum.parse(params["date_to"]).date() if params["date_to"] else end
        if start > end:
            raise ValueError("Invalid backfill window")
        return dict(start=str(start), end=str(end), cutoff=context["data_interval_end"].in_timezone("UTC").isoformat())
    resolved = window()
    compute = BashOperator(task_id="compute_and_validate", bash_command="bash /opt/snow/tools/run_batch.sh",
        env={"SNOW_DATE_FROM": "{{ ti.xcom_pull(task_ids='window')['start'] }}",
             "SNOW_DATE_TO": "{{ ti.xcom_pull(task_ids='window')['end'] }}",
             "SNOW_CUTOFF": "{{ ti.xcom_pull(task_ids='window')['cutoff'] }}",
             "SNOW_RUN_ID": "{{ dag_run.run_id | replace(':','-') | replace('+','-') | replace('.','-') }}"}, append_env=True)
    resolved >> compute


snow_daily()
