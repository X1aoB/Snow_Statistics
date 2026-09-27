"""Capture explicit public product versions without adding product dependencies."""
import argparse
import json
from pathlib import Path

from snow_statistics.public_catalog import (
    capture_releases,
    collector_allowlist_from_env,
    compare_collector,
    make_snapshot,
    save_snapshot,
)

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--output", required=True, help="Independent catalog directory")
parser.add_argument("--collector-env", required=True, help="Read only three public allowlists; credentials are never output")
commands = parser.add_subparsers(dest="command", required=True)
seed = commands.add_parser("from-json", help="Read explicitly supplied public paths/IDs and release declarations")
seed.add_argument("--input", required=True)
capture = commands.add_parser("capture", help="Read exact committed public directories, never the dirty working tree")
capture.add_argument("--stage", choices=("candidate", "published"), required=True)
for app in ("mywebsite", "project-snow"):
    capture.add_argument("--" + app + "-repo", required=True)
    capture.add_argument("--" + app + "-sha", required=True)
    capture.add_argument("--" + app + "-release-at", help="Actual release time; required only for published")
args = parser.parse_args()
if args.command == "from-json":
    path = Path(args.input)
    if path.stat().st_size > 262144:
        raise ValueError("Public catalog input is too large")
    snapshot = make_snapshot(json.loads(path.read_bytes()))
else:
    snapshot = capture_releases(
        {app: getattr(args, app + "_repo") for app in ("mywebsite", "project_snow")},
        {app: getattr(args, app + "_sha") for app in ("mywebsite", "project_snow")}, stage=args.stage,
        release_times={app: getattr(args, app + "_release_at") for app in ("mywebsite", "project_snow")})
comparison = compare_collector(snapshot, collector_allowlist_from_env(Path(args.collector_env).read_text(encoding="utf-8")))
print(json.dumps(comparison, sort_keys=True))
if not comparison["exact_match"]:
    raise SystemExit("Catalog and collector allowlists differ; no snapshot was written")
target = save_snapshot(args.output, snapshot)
print(json.dumps(dict(file=str(target), stage=snapshot["stage"], snapshot_id=snapshot["snapshot_id"],
                      evidence="explicit_release_declaration")))
