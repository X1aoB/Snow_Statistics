import argparse
import json

from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--run", required=True)
parser.add_argument("--ops", required=True)
parser.add_argument("--expected", required=True)
args = parser.parse_args()
spark = SparkSession.builder.appName("snow-spark-reconcile").getOrCreate()
expected = json.load(open(args.expected, encoding="utf-8"))
actual = []
for row in spark.read.parquet(args.run + "/ads_daily").collect():
    item = row.asDict()
    item["date"] = item.pop("business_date").isoformat()
    actual.append(item)
def sort(rows):
    return sorted(rows, key=lambda row: (row["source"], row["app"], row["date"]))
assert sort(actual) == sort(expected["daily"]), "Spark/lite oracle integer mismatch"
rounds = spark.read.parquet(args.ops + "/fact_ticket_round").collect()
assert len(rounds) == len(expected["ticket_rounds"])
assert sorted(row.duration_seconds for row in rounds) == sorted(row["duration_seconds"] for row in expected["ticket_rounds"])
dimensions = spark.read.parquet(args.ops + "/dim_content_scd2").collect()
assert len(dimensions) == len(expected["content_scd2"])
assert sum(row.deleted for row in dimensions) == 1
assert next(row for row in dimensions if row.key == "content-1" and row.valid_to is None).after.category == "engineering"
print(json.dumps({"integer_metrics_equal": True, "daily_rows": len(actual), "ticket_rounds": len(rounds), "content_versions": len(dimensions)}))
spark.stop()
