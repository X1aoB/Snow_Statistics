#!/usr/bin/env bash
# Direct entry point for a provisioned Spark client. Airflow uses airflow_gateway.py.
set -euo pipefail
: "${SNOW_DATE_FROM:?}" "${SNOW_DATE_TO:?}" "${SNOW_CUTOFF:?}" "${SNOW_RUN_ID:?}"
: "${SNOW_ODS_PATH:?}" "${SNOW_WAREHOUSE_PATH:?}"
: "${CONTROL_IP:?}"
mkdir -p /opt/snow/runtime/publication
spark-submit --master yarn --deploy-mode client \
  --driver-memory 768m --executor-memory 768m --num-executors 1 --executor-cores 1 \
  --conf spark.executor.memoryOverhead=256 --conf spark.yarn.am.memory=512m --conf spark.yarn.am.memoryOverhead=256 \
  --conf spark.driver.host="$CONTROL_IP" --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.sql.hive.metastore.version=3.1.3 --conf spark.sql.hive.metastore.jars=path \
  --conf 'spark.sql.hive.metastore.jars.path=file:///opt/snow/runtime/hive-client/*.jar' \
  /opt/snow/warehouse/spark/batch.py --input "$SNOW_ODS_PATH" --output "$SNOW_WAREHOUSE_PATH" \
  --run-id "$SNOW_RUN_ID" --date-from "$SNOW_DATE_FROM" --date-to "$SNOW_DATE_TO" --cutoff "$SNOW_CUTOFF" \
  --source "${SNOW_SOURCE:-synthetic}" --register-hive --package-file "/opt/snow/runtime/publication/$SNOW_RUN_ID.json"
# Airflow XCom stores only this safe run token, never events or credentials.
printf '%s\n' "$SNOW_RUN_ID"
