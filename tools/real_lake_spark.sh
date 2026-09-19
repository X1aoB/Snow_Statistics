#!/usr/bin/env bash
# Dedicated aggregate-only driver, fixed image/limits and exact-container cleanup.
set -euo pipefail
test "$(hostname)" = snow-control
cd /home/snow/Snow_Statistics
test "$#" -ge 4
cidfile="$1"; driver_name="$2"; warehouse="$3"; shift 3
[[ "$cidfile" =~ ^runtime/real/lake-authority/[a-z][a-z0-9-]{2,23}/[A-Za-z0-9_-]{1,100}/[A-Za-z0-9_-]{1,60}/data/driver\.cid$ ]]
[[ "$driver_name" =~ ^snow-real-lake-[a-f0-9]{20}$ ]]
test ! -e "$cidfile"
for name in $(sudo docker ps --format '{{.Names}}'); do
  case "$name" in snow-lab-control-namenode-1|snow-lab-control-resourcemanager-1) ;;
    *) echo "Unrelated container blocks this fixed lake compute stage" >&2; exit 1 ;;
  esac
done
SPARK_IMAGE=$(sed -n 's/^SPARK_IMAGE=//p' lab/locks/images.env | tr -d '\r')
CONTROL_IP=$(sed -n 's/^CONTROL_IP=//p' lab/.env | tr -d '\r')
COMPUTE_IP=$(sed -n 's/^COMPUTE_IP=//p' lab/.env | tr -d '\r')
ANALYSIS_IP=$(sed -n 's/^ANALYSIS_IP=//p' lab/.env | tr -d '\r')
test "$SPARK_IMAGE" = 'apache/spark@sha256:936ff39fd63e2bb5ed064f0fbe1518198473f1cdbfa2f863d087a9a8e58116ba'
for ip in "$CONTROL_IP" "$COMPUTE_IP" "$ANALYSIS_IP"; do [[ "$ip" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; done
cleanup() {
  if test -f "$cidfile"; then
    cid="$(cat "$cidfile")"
    [[ "$cid" =~ ^[a-f0-9]{64}$ ]] || return 1
    if sudo docker inspect "$cid" >/dev/null 2>&1; then
      test "$(sudo docker inspect --format '{{index .Config.Labels "org.snow-statistics.lake-driver"}}' "$cid")" = "$driver_name"
      sudo docker rm -f "$cid" >/dev/null
    fi
    rm -- "$cidfile"
  fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
sudo docker run --name "$driver_name" --cidfile "$cidfile" --pull=never \
  --label "org.snow-statistics.lake-driver=$driver_name" --memory 1280m --cpus 2 --pids-limit 256 --network host --user 0:0 \
  --add-host "snow-control:$CONTROL_IP" --add-host "snow-compute:$COMPUTE_IP" --add-host "snow-analysis:$ANALYSIS_IP" \
  -e HADOOP_CONF_DIR=/etc/hadoop -e PYSPARK_PYTHON=/usr/bin/python3 \
  -v "$PWD/warehouse/spark:/opt/snow/warehouse/spark:ro" \
  -v "$PWD/lab/spark-submit-locked.sh:/opt/snow/lab/spark-submit-locked.sh:ro" \
  -v "$PWD/lab/locks:/opt/snow/lab/locks:ro" -v "$PWD/runtime/jars:/opt/snow/runtime/jars:ro" \
  -v "$PWD/$(dirname "$cidfile"):/opt/snow/$(dirname "$cidfile")" \
  -v "$PWD/lab/generated/hadoop:/etc/hadoop:ro" -w /tmp "$SPARK_IMAGE" \
  bash /opt/snow/lab/spark-submit-locked.sh --master yarn --deploy-mode client --driver-memory 640m \
  --executor-memory 512m --num-executors 1 --executor-cores 1 \
  --conf spark.executor.memoryOverhead=256 --conf spark.yarn.am.memory=256m \
  --conf spark.yarn.am.memoryOverhead=128 --conf spark.driver.host="$CONTROL_IP" \
  --conf spark.driver.bindAddress=0.0.0.0 --conf spark.yarn.submit.waitAppCompletion=true \
  --conf 'spark.yarn.jars=local:/opt/spark/jars/*' \
  --conf spark.yarn.am.clientModeTreatDisconnectAsFailed=true --conf spark.eventLog.enabled=false \
  --jars /opt/snow/runtime/jars/iceberg-spark-runtime-3.5_2.12-1.10.0.jar \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.real_lake=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.real_lake.type=hadoop \
  --conf "spark.sql.catalog.real_lake.warehouse=$warehouse" "$@"
