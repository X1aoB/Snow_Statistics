"""Synthetic Doris acceptance on the isolated analysis VM, never a public service."""
import argparse
import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path

import pymysql

parser = argparse.ArgumentParser()
parser.add_argument("--host", default="127.0.0.1")
args = parser.parse_args()
root = Path(__file__).resolve().parents[1]


def connect():
    return pymysql.connect(host=args.host, port=9030, user=os.getenv("DORIS_USER", "root"),
                           password=os.getenv("DORIS_PASSWORD", ""), autocommit=True, connect_timeout=3)


deadline = time.monotonic() + 180
while True:
    try:
        db = connect()
        with db.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute("SHOW BACKENDS")
            if any(str(row["Alive"]).lower() == "true" or row["Alive"] == 1 for row in cursor.fetchall()):
                break
        db.close()
    except pymysql.MySQLError:
        pass
    if time.monotonic() > deadline:
        raise SystemExit("Doris FE/BE did not become ready within 180 seconds")
    time.sleep(2)

with db:
    with db.cursor() as cursor:
        schema = (root / "warehouse/doris/schema.sql").read_text().replace(
            "DATABASE IF NOT EXISTS snow;", "DATABASE IF NOT EXISTS snow_acceptance;").replace("snow.", "snow_acceptance.")
        for statement in schema.split(";"):
            if statement.strip():
                cursor.execute(statement)
        # Stable keys keep repeat acceptance runs idempotent. No real source rows are touched.
        for version in (1, 2, 1):
            cursor.execute("""INSERT INTO snow_acceptance.events_realtime
                VALUES ('synthetic','mywebsite','event:fixture-doris-page','page_view','2026-09-11',
                '00000000-0000-4000-8000-000000000001','/',NULL,NULL,
                '2026-09-11 01:00:00','2026-09-11 01:00:01',%s)""", (version,))
            cursor.execute("""INSERT INTO snow_acceptance.daily_offline VALUES
                ('synthetic','mywebsite','2026-09-11',1,1,0,0,%s,'2026-09-11 01:00:01')""", (version,))
        cursor.execute("""SELECT COUNT(*),MAX(business_version) FROM snow_acceptance.events_realtime
            WHERE source='synthetic' AND business_key='event:fixture-doris-page'""")
        assert cursor.fetchone() == (1, 2)
        cursor.execute("""SELECT business_version FROM snow_acceptance.daily_offline
            WHERE source='synthetic' AND app='mywebsite' AND business_date='2026-09-11'""")
        assert cursor.fetchone()[0] == 2
        # Reconciliation is scoped to the single dedicated fixture key/date.
        cursor.execute("""SELECT SUM(IF(event_type='page_view',1,0)),COUNT(DISTINCT anonymous_id),
            SUM(IF(event_type='request_complete',1,0)),SUM(IF(event_type='request_complete' AND success=TRUE,1,0))
            FROM snow_acceptance.events_realtime WHERE source='synthetic' AND business_key='event:fixture-doris-page'""")
        actual = tuple(int(n) for n in cursor.fetchone())
        cursor.execute("""SELECT pv,uv,requests,successes FROM snow_acceptance.daily_offline
            WHERE source='synthetic' AND app='mywebsite' AND business_date='2026-09-11'""")
        assert actual == tuple(cursor.fetchone()) == (1, 1, 0, 0)
        cursor.execute("SELECT VERSION()")
        version = cursor.fetchone()[0]
    with db.cursor(pymysql.cursors.DictCursor) as cursor:
        cursor.execute("SHOW BACKENDS")
        backend_version = cursor.fetchall()[0]["Version"]
receipt = dict(engine="Doris", mysql_compatibility_version=version, backend_version=backend_version,
               source="synthetic", scope="single_key_fixture", rows=1,
               stable_key_replay=True, stale_business_version_ignored=True, integer_metrics_equal=True,
               measured_at=datetime.now(UTC).isoformat())
(root / "runtime").mkdir(exist_ok=True)
(root / "runtime/doris-receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
print(json.dumps(receipt, indent=2))
