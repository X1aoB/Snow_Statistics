#!/usr/bin/env bash
set -euo pipefail
cd /home/snow/Snow_Statistics
set -a
. lab/locks/images.env
. lab/.env
set +a
test -f lab/locks/hive-client.sha256
(cd runtime/hive-client && sha256sum --status -c ../../lab/locks/hive-client.sha256)
test "$(find runtime/hive-client -maxdepth 1 -name '*.jar' | wc -l)" -eq "$(wc -l < lab/locks/hive-client.sha256)"
sudo docker run --rm --name snow-spark-yarn --memory 1792m --cpus 2 --network host --user 0:0 \
  --add-host "snow-control:$CONTROL_IP" --add-host "snow-compute:$COMPUTE_IP" --add-host "snow-analysis:$ANALYSIS_IP" \
  -e HADOOP_CONF_DIR=/etc/hadoop -e PYSPARK_PYTHON=/usr/bin/python3 \
  -v "$PWD:/opt/snow" -v "$PWD/lab/generated/hadoop:/etc/hadoop:ro" -w /tmp "$SPARK_IMAGE" \
  bash /opt/snow/lab/spark-submit-locked.sh --master yarn --deploy-mode client --driver-memory 768m \
  --executor-memory 768m --num-executors 1 --executor-cores 1 \
  --conf spark.executor.memoryOverhead=256 --conf spark.yarn.am.memory=512m \
  --conf spark.yarn.am.memoryOverhead=256 --conf spark.driver.host="$CONTROL_IP" \
  --conf spark.driver.bindAddress=0.0.0.0 --conf spark.yarn.submit.waitAppCompletion=true \
  --conf 'spark.yarn.jars=local:/opt/spark/jars/*' \
  --conf spark.yarn.am.clientModeTreatDisconnectAsFailed=true \
  --conf spark.sql.hive.metastore.version=3.1.3 --conf spark.sql.hive.metastore.jars=path \
  --conf 'spark.sql.hive.metastore.jars.path=file:///opt/snow/runtime/hive-client/*.jar' \
  "$@"
