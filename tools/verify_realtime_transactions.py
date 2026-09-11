"""Doris 2PC visibility and commit replay on a duplicate-key synthetic probe table."""
import argparse
import json
import re
import time
from pathlib import Path
from uuid import uuid4

import httpx
import pymysql

from snow_statistics.io import write_json

parser = argparse.ArgumentParser()
parser.add_argument("--host", default="192.168.216.133")
parser.add_argument("--lane", required=True)
args = parser.parse_args()
if not re.fullmatch(r"[a-z0-9_]{1,24}", args.lane):
    parser.error("Invalid synthetic lane")
database = "snow_realtime_" + args.lane
folder = Path("runtime/realtime") / args.lane


def sql(statement):
    with pymysql.connect(host=args.host, port=9030, user="root", password="", autocommit=True, connect_timeout=5) as db, db.cursor() as cursor:
        cursor.execute(statement)
        return cursor.fetchall()


def transaction(txn_id):
    with pymysql.connect(host=args.host, port=9030, user="root", password="", autocommit=True, connect_timeout=5) as db, db.cursor(pymysql.cursors.DictCursor) as cursor:
        cursor.execute(f"SHOW TRANSACTION FROM {database} WHERE ID={int(txn_id)}")
        return cursor.fetchone()


sql(f"CREATE TABLE IF NOT EXISTS {database}.txn_probe (id BIGINT, value BIGINT) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')")
receipt_path = folder / "transactions.json"
receipt = json.loads(receipt_path.read_bytes()) if receipt_path.exists() else dict(engine="Doris 3.0.6.2", table=database + ".txn_probe", key_model="DUPLICATE", attempts=[])
if sql(f"SELECT COUNT(*) FROM {database}.txn_probe")[0][0]:
    if any(a["operation"] == "commit" and a.get("repeated_commit_did_not_duplicate") for a in receipt["attempts"]):
        assert sql(f"SELECT id,value FROM {database}.txn_probe") == ((2, 200),)
        print(json.dumps(receipt))
        raise SystemExit(0)
    raise ValueError("Probe table already has evidence; preserve it and choose a new lane")
with httpx.Client(base_url="http://" + args.host + ":8040", auth=("root", ""), timeout=30, trust_env=False) as client:
    for value, operation in ((1, "abort"), (2, "commit")):
        if any(a["operation"] == operation for a in receipt["attempts"]):
            continue
        pending = folder / ("txn-prepare-" + operation + ".json")
        if pending.exists():
            prepared = json.loads(pending.read_bytes())
            label = prepared["Label"]
        else:
            label = "snow-probe-" + args.lane + "-" + uuid4().hex
            response = client.put(f"/api/{database}/txn_probe/_stream_load",
                                  headers={"label": label, "format": "json", "read_json_by_line": "true", "two_phase_commit": "true"},
                                  content=json.dumps(dict(id=value, value=value * 100)) + "\n")
            response.raise_for_status()
            prepared = response.json()
            write_json(pending, prepared)
        assert prepared["Status"] == "Success" and prepared["NumberLoadedRows"] == 1
        prepared_state = transaction(prepared["TxnId"])
        write_json(folder / ("txn-state-before-" + operation + ".json"), prepared_state)
        assert prepared_state["TransactionStatus"] == "PRECOMMITTED", prepared_state
        assert sql(f"SELECT COUNT(*) FROM {database}.txn_probe")[0][0] == 0
        headers = {"txn_id": str(prepared["TxnId"]), "txn_operation": operation}
        response = client.put(f"/api/{database}/_stream_load_2pc", headers=headers)
        response.raise_for_status()
        result = response.json()
        assert result["status"] == "Success", result
        expected = 0 if operation == "abort" else 1
        deadline = time.monotonic() + 30
        while sql(f"SELECT COUNT(*) FROM {database}.txn_probe")[0][0] != expected:
            if time.monotonic() >= deadline:
                raise RuntimeError("Transaction visibility timed out")
            time.sleep(0.2)
        item = dict(operation=operation, txn_id=prepared["TxnId"], label=label,
                    loaded_rows=1, visible_before_terminal=0, visible_after_terminal=expected, response=result,
                    prepared_state=prepared_state, terminal_state=transaction(prepared["TxnId"]))
        if operation == "commit":
            replay = client.put(f"/api/{database}/_stream_load_2pc", headers=headers)
            replay.raise_for_status()
            item["replay_response"] = replay.json()
            assert sql(f"SELECT id,value FROM {database}.txn_probe") == ((2, 200),)
            item["repeated_commit_did_not_duplicate"] = True
        receipt["attempts"].append(item)
        write_json(folder / "transactions.json", receipt)
print(json.dumps(receipt))
