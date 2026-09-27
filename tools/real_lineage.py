"""Inspect or deliver only the dedicated real metadata journal."""
import argparse
import json

from snow_statistics.real_lineage import RealJournal, flush_real

parser = argparse.ArgumentParser()
parser.add_argument("--journal", required=True)
parser.add_argument("--target", default="real-marquez")
parser.add_argument("--url", help="Explicit loopback Marquez tunnel; omitted means read status only")
args = parser.parse_args()
journal = RealJournal(args.journal)
if args.url:
    print(json.dumps({"delivered": flush_real(journal, args.url, args.target)}))
print(json.dumps(journal.status(args.target), sort_keys=True))
