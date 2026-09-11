"""External log follower. Read a timestamped log stream; emit only safe events.

Example source: docker logs --follow --timestamps PRODUCT_CONTAINER | this script.
This separate reader must not replace or become a dependency of business logging.
"""
import argparse
import json
import os
import sys
from datetime import datetime

import httpx

from snow_statistics.log_adapter import from_log

parser = argparse.ArgumentParser()
parser.add_argument("--url", required=True)
parser.add_argument("--enabled", action="store_true", help="Default is disabled")
args = parser.parse_args()
if not args.enabled:
    raise SystemExit("Log forwarding disabled")
token = os.environ.get("SNOW_SERVER_TOKEN", "")
if not token:
    raise SystemExit("SNOW_SERVER_TOKEN required")
with httpx.Client(timeout=2, headers={"Authorization": "Bearer " + token}) as client:
    for line in sys.stdin:
        if len(line) > 65536 or '"public_generation_complete"' not in line:
            continue
        try:
            timestamp = line.split(" ", 1)[0]
            datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            offset = line.index('{"event"')
            record = json.loads(line[offset:])
            event = from_log(record, timestamp)
            if event is None:
                continue
            for attempt in range(2):
                try:
                    response = client.post(args.url.rstrip("/") + "/analytics/v1/events", json={"events": [event]})
                    if response.status_code < 500 and response.status_code != 429:
                        break
                except httpx.HTTPError:
                    pass
        except (ValueError, KeyError, TypeError):
            continue  # Never print the rejected original log line.
