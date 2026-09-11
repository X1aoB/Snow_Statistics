#!/usr/bin/env bash
set -euo pipefail
: "${SNOW_DATE_FROM:?}" "${SNOW_DATE_TO:?}" "${SNOW_CUTOFF:?}" "${SNOW_RUN_ID:?}"
: "${SNOW_ODS_PATH:?}" "${SNOW_WAREHOUSE_PATH:?}"
exec spark-submit --master yarn --deploy-mode client \
  --driver-memory 768m --executor-memory 1g --num-executors 1 --executor-cores 1 \
  --conf spark.sql.hive.metastore.version=3.1.3 --conf spark.sql.hive.metastore.jars=maven \
  /opt/snow/warehouse/spark/batch.py --input "$SNOW_ODS_PATH" --output "$SNOW_WAREHOUSE_PATH" \
  --run-id "$SNOW_RUN_ID" --date-from "$SNOW_DATE_FROM" --date-to "$SNOW_DATE_TO" --cutoff "$SNOW_CUTOFF"
