"""Inspect/replay the dedicated journal; API outage never enters compute requests."""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from snow_statistics.io import write_json  # noqa: E402
from snow_statistics.lineage import Journal, flush  # noqa: E402

parser = argparse.ArgumentParser()
parser.add_argument("action", choices=("status", "backup", "flush", "export-acks", "import-acks", "reconcile-failed"))
parser.add_argument("--database", type=Path, default=Path("runtime/publication/lineage.sqlite"))
parser.add_argument("--target", default="snow-lab-marquez-v1", help="Change after replacing the Marquez database")
parser.add_argument("--url", default="http://127.0.0.1:5000")
parser.add_argument("--limit", type=int, default=100)
parser.add_argument("--receipt", type=Path)
parser.add_argument("--snapshot", type=Path)
parser.add_argument("--fail-after-send", action="store_true")
args = parser.parse_args()
journal = Journal(args.database)
if args.action == "backup":
    if not args.snapshot:
        parser.error("--snapshot is required")
    journal.backup(args.snapshot)
elif args.action == "flush":
    flush(journal, args.url, args.target, args.limit, fail_after_send=args.fail_after_send)
elif args.action == "export-acks":
    if not args.receipt:
        parser.error("--receipt is required")
    write_json(args.receipt, journal.receipt(args.target))
elif args.action in ("import-acks", "reconcile-failed"):
    if not args.receipt:
        parser.error("--receipt is required")
    receipt = json.loads(args.receipt.read_bytes())
    if args.action == "import-acks":
        if receipt["target"] != args.target:
            raise ValueError("Different Marquez target")
        journal.acknowledge(receipt)
    else:
        journal.reconcile_failed(receipt)
print(json.dumps(journal.status(args.target)))
