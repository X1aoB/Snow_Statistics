-- Configure catalog snow with SparkCatalog, type=hadoop and an independent warehouse.
-- Spark 3.5 runtime JAR: iceberg-spark-runtime-3.5_2.12:1.10.0.
CREATE NAMESPACE IF NOT EXISTS snow.lab;
CREATE TABLE IF NOT EXISTS snow.lab.daily (
  source STRING, app STRING, business_date DATE,
  pv BIGINT, uv BIGINT, requests BIGINT, successes BIGINT
) USING iceberg PARTITIONED BY (source, months(business_date));
-- Replace INPUT_PARQUET with the accepted run's ADS output before execution.
CREATE OR REPLACE TEMP VIEW accepted_daily USING parquet OPTIONS (path 'INPUT_PARQUET');
MERGE INTO snow.lab.daily t USING accepted_daily s
ON t.source=s.source AND t.app=s.app AND t.business_date=s.business_date
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
ALTER TABLE snow.lab.daily ADD COLUMN quality_version STRING;
ALTER TABLE snow.lab.daily REPLACE PARTITION FIELD months(business_date) WITH days(business_date);
SELECT * FROM snow.lab.daily.snapshots;
SELECT * FROM snow.lab.daily.files;
CALL snow.system.rewrite_data_files(table => 'lab.daily');
-- Expire snapshots/remove orphan files only after the documented reader-retention gate.
