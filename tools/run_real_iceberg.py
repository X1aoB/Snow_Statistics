"""Prepare or execute a registered real aggregate-only Iceberg experiment."""
import argparse
import json
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

from snow_statistics.io import atomic_write, digest
from snow_statistics.lifecycle import RealLifecycle, timestamp
from snow_statistics.publication import canonical
from snow_statistics.real_lake import prepare_bundle, validate_receipt
from snow_statistics.real_lineage import RealCapture, RealJournal, dataset
from snow_statistics.real_remote_lifecycle import RealRemoteLifecycle

parser = argparse.ArgumentParser()
for name in ("daily", "behavior", "registry", "runtime-root", "warehouse", "run-id"):
    parser.add_argument("--" + name, required=True)
parser.add_argument("--prepare-only", action="store_true", help="Reserve all copies, then run actual lifecycle cleanup before execution")
parser.add_argument("--lineage-db")
args = parser.parse_args()
root = Path(__file__).resolve().parents[1]
daily = json.loads(Path(args.daily).read_bytes())
behavior = json.loads(Path(args.behavior).read_bytes())
bundle = prepare_bundle(daily, behavior, args.run_id, args.warehouse)
remote = RealRemoteLifecycle(args.registry)
remote.register(args.warehouse, "aggregate", bundle["original_at"])
local_root = Path(args.runtime_root).absolute()
if not local_root.is_relative_to((root / "runtime/real/lake").absolute()):
    raise ValueError("The real aggregate bundle must stay under runtime/real/lake")
local = RealLifecycle(local_root)
if not local.owner.exists():
    local.initialize()
for path in ("input.json", "receipt.json", "spark.log"):
    local.register(path, "aggregate", bundle["original_at"])
local.cleanup()
payload = canonical(bundle)
input_file = local_root / "input.json"
if input_file.exists() and input_file.read_bytes() != payload:
    raise ValueError("A run ID cannot overwrite a different real aggregate input")
atomic_write(input_file, payload)
input_sha = digest(payload)
print(json.dumps(dict(prepared=True, source="real", run_id=args.run_id, input_sha256=input_sha,
                     warehouse=args.warehouse, expires_at=bundle["expires_at"])), flush=True)
if args.prepare_only:
    raise SystemExit(0)
if not str(root).startswith("/home/snow/"):
    raise ValueError("Execute this registered job on snow-control; Windows may prepare files only")
owner, registry = remote._read()
receipt_path = remote.directory / "last-cleanup.json"
cleanup = json.loads(receipt_path.read_bytes())
now = datetime.now(UTC)
if (remote.journal.exists() or cleanup["registry_sha256"] != digest(canonical(registry)) or
        not now - timedelta(minutes=15) <= timestamp(cleanup["checked_at"]) <= now or
        cleanup.get("next_expiry") and timestamp(cleanup["next_expiry"]) <= now):
    raise ValueError("Run actual registered remote lifecycle cleanup after preparation and before the real lake job")
name = "iceberg-spark-runtime-3.5_2.12-1.10.0.jar"
lock = json.loads((root / "lab/locks/jars.json").read_bytes())[name]
jar = root / "runtime/jars" / name
if jar.stat().st_size != lock["bytes"] or digest(jar.read_bytes()) != lock["sha256"]:
    raise ValueError("Pinned Iceberg runtime does not match its existing lock")
relative = local_root.relative_to(root).as_posix()
command = ["bash", "tools/spark_yarn_scale.sh", "--jars", "/opt/snow/runtime/jars/" + name,
           "--conf", "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
           "--conf", "spark.sql.catalog.real_lake=org.apache.iceberg.spark.SparkCatalog",
           "--conf", "spark.sql.catalog.real_lake.type=hadoop",
           "--conf", "spark.sql.catalog.real_lake.warehouse=" + args.warehouse,
           "--conf", "spark.eventLog.enabled=false", "/opt/snow/warehouse/spark/real_iceberg.py",
           "--input", "/opt/snow/" + relative + "/input.json", "--input-sha256", input_sha,
           "--receipt", "/opt/snow/" + relative + "/receipt.json"]
journal = RealJournal(args.lineage_db) if args.lineage_db else None
inputs = [dataset("file://snow-control" + input_file.as_posix())]
with RealCapture(journal, "snow_real.iceberg_aggregates", args.run_id, inputs) as capture:
    with (local_root / "spark.log").open("w", encoding="utf-8") as log:
        subprocess.run(command, cwd=root, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=1100)
    receipt = validate_receipt(json.loads((local_root / "receipt.json").read_bytes()), bundle)
    capture.outputs = [dataset(value["location"]) for value in receipt["tables"].values()]
print(json.dumps(receipt, sort_keys=True))
