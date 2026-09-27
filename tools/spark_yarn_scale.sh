#!/usr/bin/env bash
set -euo pipefail
test "$(hostname)" = snow-control
cd /home/snow/Snow_Statistics
for name in $(sudo docker ps --format '{{.Names}}'); do
  case "$name" in snow-lab-control-namenode-1|snow-lab-control-resourcemanager-1) ;;
    *) echo "Stop $name before the scale compute phase" >&2; exit 1 ;;
  esac
done
set -a
. lab/locks/images.env
. lab/.env
set +a
mkdir -p runtime/scale/eventlogs
driver_identity=()
if [[ -n "${SNOW_REAL_DRIVER_CIDFILE:-}" ]]; then
  [[ "$SNOW_REAL_DRIVER_CIDFILE" =~ ^/home/snow/Snow_Statistics/runtime/real/runs/[A-Za-z0-9_-]{1,100}/data/(daily|behavior)\.cid$ ]]
  test ! -e "$SNOW_REAL_DRIVER_CIDFILE"
  driver_identity=(--cidfile "$SNOW_REAL_DRIVER_CIDFILE")
fi
sudo docker run --rm --name snow-spark-yarn --memory 1280m --cpus 2 --network host --user 0:0 \
  "${driver_identity[@]}" \
  --add-host "snow-control:$CONTROL_IP" --add-host "snow-compute:$COMPUTE_IP" --add-host "snow-analysis:$ANALYSIS_IP" \
  -e HADOOP_CONF_DIR=/etc/hadoop -e PYSPARK_PYTHON=/usr/bin/python3 \
  -v "$PWD:/opt/snow" -v "$PWD/lab/generated/hadoop:/etc/hadoop:ro" -w /tmp "$SPARK_IMAGE" \
  bash /opt/snow/lab/spark-submit-locked.sh --master yarn --deploy-mode client --driver-memory 640m \
  --executor-memory 512m --num-executors 1 --executor-cores 1 \
  --conf spark.executor.memoryOverhead=256 --conf spark.yarn.am.memory=256m \
  --conf spark.yarn.am.memoryOverhead=128 --conf spark.driver.host="$CONTROL_IP" \
  --conf spark.driver.bindAddress=0.0.0.0 --conf spark.yarn.submit.waitAppCompletion=true \
  --conf 'spark.yarn.jars=local:/opt/spark/jars/*' \
  --conf spark.yarn.am.clientModeTreatDisconnectAsFailed=true \
  --conf spark.eventLog.enabled=true --conf spark.eventLog.compress=false \
  --conf spark.eventLog.dir=file:///opt/snow/runtime/scale/eventlogs \
  "$@"
