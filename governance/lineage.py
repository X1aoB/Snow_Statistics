"""Explicit table-level OpenLineage evidence. No unverified column lineage claims."""
import argparse
import uuid
from datetime import UTC, datetime

import httpx

parser = argparse.ArgumentParser()
parser.add_argument("--url", default="http://127.0.0.1:5000")
parser.add_argument("--job", required=True)
parser.add_argument("--run-id", required=True, type=uuid.UUID)
parser.add_argument("--state", choices=["START", "COMPLETE", "FAIL"], required=True)
parser.add_argument("--input", action="append", default=[])
parser.add_argument("--output", action="append", default=[])
args = parser.parse_args()
payload = dict(eventType=args.state, eventTime=datetime.now(UTC).isoformat(),
               producer="https://github.com/X1aoB/Snow_Statistics", schemaURL="https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
               run={"runId": str(args.run_id)}, job={"namespace": "snow-statistics", "name": args.job},
               inputs=[{"namespace": "snow-lab", "name": name} for name in args.input],
               outputs=[{"namespace": "snow-lab", "name": name} for name in args.output])
response = httpx.post(args.url.rstrip("/") + "/api/v1/lineage", json=payload, timeout=10)
response.raise_for_status()
print("Submitted table-level " + args.state)
