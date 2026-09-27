"""Fixed catalog worker. Input contains metadata and hashes, never event rows."""
import argparse
import hashlib
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from snow_statistics.real_hive_contract import canonical, operate, validate_request  # noqa: E402
from snow_statistics.real_hive_spark import SparkHiveCatalog  # noqa: E402

parser = argparse.ArgumentParser()
parser.add_argument("--request", required=True)
parser.add_argument("--sha256", required=True)
args = parser.parse_args()
target = Path(args.request)
if (not re.fullmatch(r"/opt/snow/runtime/real/lifecycle/[a-z][a-z0-9-]{2,23}/hive-requests/[a-f0-9]{64}\.json", args.request) or
        target.resolve() != target.absolute() or target.stat().st_size > 8 * 1024**2):
    raise ValueError("Unexpected bounded catalog request file")
body = target.read_bytes()
if hashlib.sha256(body).hexdigest() != args.sha256:
    raise ValueError("Catalog request file checksum differs")
request = json.loads(body)
validate_request(request, now=datetime.now(timezone.utc))
if canonical(request) != body:
    raise ValueError("Catalog request must use the canonical hash contract")
spark = SparkSession.builder.appName("snow-real-hive-catalog").config("spark.hadoop.hive.metastore.uris", request["metastore_uri"]).enableHiveSupport().getOrCreate()
try:
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    if (spark.version != "3.5.7" or spark.sparkContext.master != "local[1]" or
            spark.conf.get("spark.sql.catalogImplementation") != "hive" or
            spark.conf.get("spark.sql.hive.metastore.version") != "3.1.3" or
            spark.sparkContext._jsc.hadoopConfiguration().get("hive.metastore.uris") != request["metastore_uri"]):
        raise ValueError("Actual Spark/Hive engine or remote metastore differs")
    tables = operate(request, SparkHiveCatalog(spark))
    receipt = dict(schema_version=1, source="real", action=request["action"], tables=tables,
                   request_sha256=args.sha256, engine="Spark " + spark.version, master=spark.sparkContext.master,
                   application_id=spark.sparkContext.applicationId, metastore_uri=request["metastore_uri"],
                   checked_at=datetime.now(timezone.utc).isoformat())
    print("SNOW_HIVE_RESULT=" + json.dumps(receipt, sort_keys=True))
finally:
    spark.stop()
