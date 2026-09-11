"""Publish one validated synthetic correction batch atomically and read it back."""
import argparse
import json
import re
from datetime import datetime
from pathlib import Path

import pymysql

from snow_statistics.io import write_json
from snow_statistics.publication import validate

parser = argparse.ArgumentParser()
parser.add_argument("--host", default="192.168.216.133")
parser.add_argument("--lane", required=True)
parser.add_argument("--directory", type=Path, required=True)
args = parser.parse_args()
if not re.fullmatch(r"[a-z0-9_]{1,24}", args.lane):
    parser.error("Invalid lane")
package = json.loads((args.directory / "spark-correction.json").read_bytes())
validate(package)
assert package["manifest"]["engine"] == "Spark 3.5.7" and package["manifest"]["source"] == "synthetic"
expected = json.loads((args.directory / "correction-expected.json").read_bytes())["daily"]
assert package["daily"] == expected and 0 < len(expected) <= 200
version = int(datetime.fromisoformat(package["manifest"]["cutoff"].replace("Z", "+00:00")).timestamp() * 1000)
database = "snow_realtime_" + args.lane
values = [tuple(r[k] for k in ("source", "app", "date", "pv", "uv", "requests", "successes")) + (version, package["manifest"]["cutoff"].replace("T", " ").replace("+00:00", "").replace("Z", "")) for r in expected]
with pymysql.connect(host=args.host, port=9030, user="root", password="", autocommit=True, connect_timeout=5) as db, db.cursor() as cursor:
    cursor.execute(f"SELECT MAX(business_version) FROM {database}.daily_offline")
    previous = cursor.fetchone()[0]
    if previous is not None:
        if previous != version:
            raise ValueError("This acceptance lane already has another immutable correction")
        cursor.execute(f"SELECT source,app,business_date,pv,uv,requests,successes FROM {database}.daily_offline ORDER BY source,business_date,app")
        existing = [dict(source=r[0], app=r[1], date=str(r[2]), pv=r[3], uv=r[4], requests=r[5], successes=r[6]) for r in cursor.fetchall()]
        if existing != expected:
            raise ValueError("Conflicting correction replay")
    statement = f"INSERT INTO {database}.daily_offline VALUES " + ",".join(["(" + ",".join(["%s"] * 9) + ")"] * len(values))
    flat = [v for row in values for v in row]
    for _ in range(2):
        cursor.execute(statement, flat)
        cursor.execute(f"SELECT source,app,business_date,pv,uv,requests,successes FROM {database}.daily_offline ORDER BY source,business_date,app")
        actual = [dict(source=r[0], app=r[1], date=str(r[2]), pv=r[3], uv=r[4], requests=r[5], successes=r[6]) for r in cursor.fetchall()]
        assert actual == expected
    cursor.execute(f"SELECT SUM(pv) FROM {database}.daily_realtime")
    live_pv = int(cursor.fetchone()[0])
result = dict(engine="Spark 3.5.7 / Doris 3.0.6.2", source="synthetic", table=database + ".daily_offline",
              metrics=expected, version=version, repeated_publish_equal=True,
              live_pv=live_pv, corrected_pv=sum(r["pv"] for r in expected),
              note="Explicit private correction batch; public lite metrics are not served from this table")
assert result["corrected_pv"] - result["live_pv"] == 2
write_json(args.directory / "correction-published.json", result)
print(json.dumps(result))
