"""Actual subprocess death around Capture's terminal record, without an ETL claim.

The journal is an isolated lifecycle fixture, never sent to Marquez. No fabricated
Airflow failure receipt closes a process whose scheduler status is unknown.
"""
import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

from snow_statistics.io import write_json
from snow_statistics.lineage import Capture, Journal

parser = argparse.ArgumentParser()
parser.add_argument("--output", type=Path, required=True)
parser.add_argument("--worker", choices=("during", "terminal", "retry"))
args = parser.parse_args()
job = "snow_models.compute_operations"
folder = args.output.resolve()


def wait_file(path, process):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if path.exists():
            return
        if process.poll() is not None:
            raise RuntimeError("Lifecycle worker exited before the checkpoint")
        time.sleep(.05)
    raise RuntimeError("Lifecycle checkpoint timeout")


if args.worker:
    os.environ.update(SNOW_LINEAGE_ENABLED="true", SNOW_LINEAGE_DB=str(folder / "journal.sqlite"))
    with Capture(job, "process-probe-" + args.worker, lambda: []) as capture:
        if args.worker == "during":
            write_json(folder / "during.ready.json", {"side_effect_written": False})
            time.sleep(60)
        else:
            write_json(folder / (args.worker + ".result.json"), {"synthetic_probe_value": 42})
            if args.worker == "terminal":
                def pause_terminal():
                    write_json(folder / "terminal.ready.json", {"side_effect_written": True})
                    time.sleep(60)
                    return []
                capture.outputs(pause_terminal)
    raise SystemExit(0)

folder.mkdir(parents=True, exist_ok=False)
journal = Journal(folder / "journal.sqlite")
results = []
for phase in ("during", "terminal", "retry"):
    with (folder / (phase + ".log")).open("wb") as log:
        child = subprocess.Popen([sys.executable, __file__, "--output", str(folder), "--worker", phase],
                                 stdout=log, stderr=subprocess.STDOUT,
                                 creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
        try:
            if phase != "retry":
                wait_file(folder / (phase + ".ready.json"), child)
                child.kill()
            code = child.wait(timeout=15)
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=5)
    states = [e[1]["eventType"] for e in journal.pending("probe")
              if e[1]["run"]["runId"] == journal.status("probe")["open_runs"][0]["run_id"]] if phase == "during" else []
    if phase == "during":
        assert states == ["START"] and code != 0
    if phase == "terminal":
        assert len(journal.status("probe")["open_runs"]) == 2 and code != 0
    if phase == "retry":
        assert code == 0
    results.append(dict(phase=phase, exit_code=code, status=journal.status("probe")))
events = [e[1] for e in journal.pending("probe")]
assert [e["eventType"] for e in events] == ["START", "START", "START", "COMPLETE"]
assert all(not e["outputs"] for e in events)
assert json.loads((folder / "retry.result.json").read_text()) == json.loads((folder / "terminal.result.json").read_text())
journal.backup(folder / "export.sqlite")
assert Journal(folder / "export.sqlite").pending("probe") == journal.pending("probe")
result = dict(scope="Actual local subprocess lifecycle fixture, not Airflow/Spark execution or Marquez extension",
              results=results, killed_attempts_remain_open=2, false_complete=0,
              side_effect_does_not_prove_completion=True, new_attempt_complete=True,
              consistent_export=True, outputs_declared=0)
write_json(folder / "accepted.json", result)
print(json.dumps(result))
