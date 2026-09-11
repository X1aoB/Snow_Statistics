"""Compare a synthetic two-user ODS model release with independent loop models."""
import argparse
import json
from collections import Counter
from pathlib import Path

from snow_statistics.behavior import analyze
from snow_statistics.model import build
from snow_statistics.model_publication import validate_model
from snow_statistics.publication import canonical
from snow_statistics.simulator import generate

parser = argparse.ArgumentParser()
parser.add_argument("package", type=Path)
parser.add_argument("--users", type=int, default=2)
args = parser.parse_args()
release = json.loads(args.package.read_bytes())
operations = validate_model(release["operations"], "operations")
behavior = validate_model(release["behavior"], "behavior")
fixture = generate(users=args.users)
reference = analyze(fixture["events"], behavior["date_from"], behavior["date_to"], behavior["cutoff"])
for table, actual in release["behavior"]["aggregates"].items():
    assert sorted(actual, key=canonical) == sorted(reference[table], key=canonical), table
assert behavior["counts"] == {k: len(reference[k]) for k in behavior["counts"]}
model = build(fixture, operations_as_of=operations["date_to"])
tickets = Counter((r["source"], r["date"], r["status"]) for r in model["ticket_daily"] if operations["date_from"] <= r["date"] <= operations["date_to"])
categories = Counter((r["source"], r["attributes"]["category"]) for r in model["content_scd2"] if not r["deleted"] and r["valid_to"] is None)
expected = dict(ticket_daily=[dict(source=k[0], date=k[1], status=k[2], tickets=n) for k, n in tickets.items()],
                current_categories=[dict(source=k[0], category=k[1], contents=n) for k, n in categories.items()])
for table, actual in release["operations"]["aggregates"].items():
    assert sorted(actual, key=canonical) == sorted(expected[table], key=canonical), table
assert operations["counts"] == dict(dim_content_scd2=len(model["content_scd2"]), fact_ticket_round=len(model["ticket_rounds"]), fact_ticket_daily=sum(tickets.values()))
print(json.dumps(dict(golden_aggregates_equal=True, source="synthetic", users=args.users, content_hash=release["content_hash"],
                      operations=operations, behavior=behavior), indent=2))
