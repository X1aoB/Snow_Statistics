"""Replay only generated business transactions into the dedicated lab MySQL."""
import argparse
import os
from datetime import datetime

import pymysql

from snow_statistics.simulator import generate

parser = argparse.ArgumentParser()
parser.add_argument("--host", default="127.0.0.1")
parser.add_argument("--seed", type=int, default=42)
parser.add_argument("--users", type=int, default=100)
args = parser.parse_args()
fixture = generate(args.seed, args.users)
run_id = f"seed-{args.seed}-users-{args.users}-v1"
db = pymysql.connect(host=args.host, user="snow_simulator", password=os.environ["LAB_MYSQL_PASSWORD"], database="snow_ops")
try:
    for change in sorted(fixture["changes"], key=lambda c: (c["at"], c["version"])):
        with db.cursor() as cursor:
            cursor.execute("INSERT IGNORE INTO applied_transactions VALUES(%s,%s)", (run_id, change["version"]))
            if not cursor.rowcount:
                db.rollback()
                continue
            table = change["table"]
            assert table in {"contents", "campaigns", "tickets"}
            if change["op"] == "d":
                # Preserve the synthetic effective delete time in the before image.
                # Both binlog records belong to this same committed transaction.
                cursor.execute(f"UPDATE {table} SET version=%s,updated_at=%s WHERE id=%s",
                               (change["version"], datetime.fromisoformat(change["at"]).replace(tzinfo=None), change["key"]))
                cursor.execute(f"DELETE FROM {table} WHERE id=%s", (change["key"],))
            else:
                fields = dict(id=change["key"], **change["after"], version=change["version"], updated_at=datetime.fromisoformat(change["at"]).replace(tzinfo=None))
                columns = list(fields)
                updates = ",".join(f"{c}=VALUES({c})" for c in columns if c != "id")
                cursor.execute(f"INSERT INTO {table} ({','.join(columns)}) VALUES ({','.join(['%s'] * len(columns))}) ON DUPLICATE KEY UPDATE {updates}", list(fields.values()))
        db.commit()
finally:
    db.close()
