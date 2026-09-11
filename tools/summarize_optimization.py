"""Summarize completed comparisons with per-job-group task evidence, never environment secrets."""
import argparse
import json
import statistics
from pathlib import Path

from snow_statistics.io import digest, write_json
from snow_statistics.spark_evidence import job_groups, summarize

parser = argparse.ArgumentParser()
parser.add_argument("--directory", type=Path, required=True)
parser.add_argument("--eventlog", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()
accepted = json.loads((args.directory / "accepted.json").read_bytes())
setup = json.loads((args.directory / "setup.json").read_bytes())
with args.eventlog.open(encoding="utf-8") as stream:
    events = [json.loads(line) for line in stream]
application, _ = summarize(events)
assert application["application_id"] == accepted["application_id"]
assert set(application["task_outcomes"]) == {"Success"}
groups = job_groups(events)
cases, plans = {}, {}
for name in accepted["cases"]:
    case = json.loads((args.directory / (name + ".json")).read_bytes())
    metrics = groups[name]
    assert set(metrics["outcomes"]) == {"Success"}
    base = name.rsplit("-", 1)[0]
    scans = [n for n in case["operator_metrics"] if n["node"].startswith("Scan")]
    cases.setdefault(base, []).append(dict(name=name, seconds=case["seconds"], task_metrics=metrics,
                                           scans=scans, result_sha256=digest(json.dumps(case["rows"], sort_keys=True).encode())))
    plans.setdefault(base, case["physical_plan"])
assert "SortMergeJoin" in plans["join_merge"] and "BroadcastHashJoin" in plans["join_broadcast"]
assert "salt" in plans["join_salted"]
summaries = {}
for name, rows in cases.items():
    assert len(rows) == 3 and len({r["result_sha256"] for r in rows}) == 1
    summaries[name] = dict(median_seconds=statistics.median(r["seconds"] for r in rows),
                           input_bytes=[r["task_metrics"]["input_bytes"] for r in rows],
                           shuffle_write_bytes=[r["task_metrics"]["shuffle_write_bytes"] for r in rows])
result = dict(accepted=accepted, application=application, setup=setup, summaries=summaries, cases=cases,
              artifact_sha256={str(p): digest(p.read_bytes()) for p in [args.eventlog, *sorted(args.directory.glob("*.json"))]})
write_json(args.output, result)
write_json(args.output.with_name(args.output.stem + "-plans.json"), plans)
print(json.dumps(summaries, indent=2))
