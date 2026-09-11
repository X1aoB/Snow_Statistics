"""Real Doris replay/failure/correction checks, using only a synthetic Spark export."""
import argparse
import copy
import json
from pathlib import Path

from snow_statistics.publication import connect, publish, read_published

parser = argparse.ArgumentParser()
parser.add_argument("package", type=Path)
args = parser.parse_args()
package = json.loads(args.package.read_text())
assert package["manifest"]["source"] == "synthetic"
root = Path(__file__).resolve().parents[1]
lock = root / "runtime/publication"
with connect() as db:
    with db.cursor() as cursor:
        for statement in (root / "warehouse/doris/publication.sql").read_text().split(";"):
            if statement.strip():
                cursor.execute(statement)
    receipt = publish(db, package, lock)
    first = read_published(db)
    publish(db, package, lock)
    assert first == read_published(db)
    assert sorted(first["daily"], key=lambda r: (r["date"], r["app"])) == package["daily"]
    correction = copy.deepcopy(package)
    correction["manifest"].update(run_id="synthetic-correction-test", cutoff="2026-01-06T00:00:00Z")
    correction["daily"][0]["pv"] += 1
    try:
        publish(db, correction, lock, fail_after_load=True)
        raise AssertionError("Expected failure before commit")
    except RuntimeError as error:
        assert "Injected failure" in str(error)
    assert first == read_published(db), "Failure exposed uncommitted snapshot"
    # Use a separate future synthetic date for empty-day and stale-cutoff checks,
    # leaving the golden report's dates and cutoff intact for Airflow replay.
    correction["manifest"].update(date_from="2026-01-10", date_to="2026-01-10", cutoff="2026-01-11T00:00:00Z")
    correction["daily"] = [correction["daily"][0] | {"date": "2026-01-10"}]
    existing = read_published(db)["releases"]
    if not any(r["date"] == "2026-01-10" and r["cutoff"] >= "2026-01-12" for r in existing):
        publish(db, correction, lock)
    empty = copy.deepcopy(correction)
    empty["manifest"].update(run_id="synthetic-empty-day-test", cutoff="2026-01-12T00:00:00Z")
    empty["daily"] = []
    publish(db, empty, lock)
    assert all(r["date"] != "2026-01-10" for r in read_published(db)["daily"])
    try:
        publish(db, correction, lock)
        raise AssertionError("Expected stale-cutoff rejection")
    except ValueError as error:
        assert "newer cutoff" in str(error)
    conflicting = copy.deepcopy(package)
    conflicting["daily"][0]["pv"] += 1
    try:
        publish(db, conflicting, lock)
        raise AssertionError("Expected equal-cutoff conflict")
    except ValueError as error:
        assert "conflicting results" in str(error)
receipt.update(engine="Doris 3.0.6.2", input_engine=package["manifest"]["engine"],
               yarn_application=package["manifest"]["application_id"], replay_equal=True,
               failed_publish_preserves_report=True, empty_correction_removes_old_rows=True,
               stale_cutoff_rejected=True, same_cutoff_conflict_rejected=True)
(root / "runtime/publication-receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
print(json.dumps(receipt, indent=2))
