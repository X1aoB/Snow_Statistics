import json

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("snow-iceberg-smoke").getOrCreate()
spark.sql("CREATE NAMESPACE IF NOT EXISTS snow.lab")
spark.sql("CREATE TABLE snow.lab.proof (id BIGINT, business_date DATE, pv BIGINT) USING iceberg PARTITIONED BY (months(business_date))")
spark.sql("INSERT INTO snow.lab.proof VALUES (1, DATE '2026-01-01', 1), (2, DATE '2026-01-02', 2)")
snapshot = spark.sql("SELECT snapshot_id FROM snow.lab.proof.snapshots ORDER BY committed_at DESC LIMIT 1").first()[0]
spark.sql("CREATE OR REPLACE TEMP VIEW updates AS SELECT CAST(1 AS BIGINT) id, DATE '2026-01-01' business_date, CAST(5 AS BIGINT) pv")
spark.sql("MERGE INTO snow.lab.proof t USING updates s ON t.id=s.id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
assert spark.sql("SELECT SUM(pv) FROM snow.lab.proof").first()[0] == 7
assert spark.sql(f"SELECT SUM(pv) FROM snow.lab.proof VERSION AS OF {snapshot}").first()[0] == 3
spark.sql("ALTER TABLE snow.lab.proof ADD COLUMN quality_version STRING")
spark.sql("ALTER TABLE snow.lab.proof REPLACE PARTITION FIELD months(business_date) WITH days(business_date)")
spark.sql("INSERT INTO snow.lab.proof VALUES (3, DATE '2026-01-03', 3, 'v1')")
spark.sql("CALL snow.system.rewrite_data_files(table => 'lab.proof')").collect()
count = spark.sql("SELECT COUNT(*) FROM snow.lab.proof").first()[0]
assert count == 3
print(json.dumps({"iceberg": "1.10.0", "merge": True, "snapshot_read": True, "schema_evolution": True,
                  "partition_evolution": True, "rewrite_data_files": True, "rows": count}))
spark.stop()
