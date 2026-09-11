"""Read only run/task status from the dedicated Airflow SQLite metadata store."""
import argparse
import json
import sqlite3
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--run-id", required=True)
parser.add_argument("--dag-id", choices=("snow_daily", "snow_models"), default="snow_daily")
parser.add_argument("--database", type=Path, default=Path("/opt/airflow/airflow.db"))
args = parser.parse_args()
with sqlite3.connect(f"file:{args.database}?mode=ro", uri=True) as db:
    db.row_factory = sqlite3.Row
    runs = [dict(row) for row in db.execute("SELECT run_id,state,execution_date,start_date,end_date FROM dag_run WHERE dag_id=? AND run_id=?", (args.dag_id, args.run_id))]
    tasks = [dict(row) for row in db.execute("SELECT task_id,state,try_number,start_date,end_date FROM task_instance WHERE dag_id=? AND run_id=? ORDER BY task_id", (args.dag_id, args.run_id))]
print(json.dumps({"engine": "Airflow 2.10.5", "dag_id": args.dag_id, "runs": runs, "tasks": tasks}, indent=2))
