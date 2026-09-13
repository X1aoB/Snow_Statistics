"""Run only through the registered real cleanup coordinator, before model reads."""
import argparse
import json
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from real_auxiliary import prune_auxiliary_hdfs

parser = argparse.ArgumentParser()
parser.add_argument("--manifest", required=True, help="Registered local metadata file")
parser.add_argument("--output", required=True, help="Already registered fresh real HDFS directory")
args = parser.parse_args()
manifest = json.load(open(args.manifest, encoding="utf-8"))
spark = SparkSession.builder.appName("snow-real-auxiliary-retention").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
result = prune_auxiliary_hdfs(spark, manifest, args.output, datetime.now(timezone.utc))
print("SNOW_AUXILIARY_PRUNED=" + json.dumps(result, sort_keys=True))
spark.stop()
