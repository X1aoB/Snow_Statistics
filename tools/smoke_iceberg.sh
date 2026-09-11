#!/usr/bin/env bash
set -euo pipefail
cd /home/snow/Snow_Statistics
set -a
. lab/locks/images.env
set +a
warehouse="/opt/snow/runtime/iceberg-$(date -u +%Y%m%dT%H%M%S)"
sudo docker run --rm --memory 3g --cpus 2 --user 0:0 -v "$PWD:/opt/snow" -w /tmp "$SPARK_IMAGE" \
  /opt/spark/bin/spark-submit --master 'local[2]' --driver-memory 1g \
  --jars /opt/snow/runtime/jars/iceberg-spark-runtime-3.5_2.12-1.10.0.jar \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.snow=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.snow.type=hadoop --conf "spark.sql.catalog.snow.warehouse=$warehouse" \
  /opt/snow/warehouse/spark/iceberg_smoke.py > runtime/iceberg-smoke.log 2>&1
grep '^{' runtime/iceberg-smoke.log | tail -1
