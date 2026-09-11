#!/usr/bin/env bash
set -euo pipefail
cd /home/snow/Snow_Statistics
run="${1:-yarn-$(date -u +%Y%m%dT%H%M%S)}"
[[ "$run" =~ ^[a-zA-Z0-9_-]+$ ]] || exit 2
.venv/bin/python tools/prepare_spark_fixture.py
nn=snow-lab-control-namenode-1
timeout 120 bash -c 'until sudo docker exec snow-lab-control-namenode-1 hdfs dfsadmin -safemode get >/dev/null 2>&1; do sleep 2; done'
timeout 120 sudo docker exec "$nn" hdfs dfsadmin -safemode wait
sudo docker exec "$nn" hdfs dfs -mkdir -p /snow/ods/synthetic/events /snow/warehouse /snow/yarn-logs /user/root
sudo docker cp runtime/spark-fixture/events.jsonl "$nn:/tmp/snow-events.jsonl"
sudo docker exec "$nn" hdfs dfs -test -e /snow/ods/synthetic/events/fixture.jsonl || \
  sudo docker exec "$nn" hdfs dfs -put /tmp/snow-events.jsonl /snow/ods/synthetic/events/fixture.jsonl
# Wait for actual two-copy placement before calling this a replicated input.
timeout 120 sudo docker exec "$nn" hdfs dfs -setrep -w 2 /snow/ods/synthetic/events/fixture.jsonl
timeout 600 bash tools/spark_yarn.sh /opt/snow/warehouse/spark/batch.py \
  --input /snow/ods/synthetic/events/fixture.jsonl --output /snow/warehouse \
  --run-id "$run" --date-from 2026-01-01 --date-to 2026-01-04 --cutoff 2026-01-05T00:00:00Z \
  --source synthetic --register-hive --expected /opt/snow/runtime/spark-fixture/expected.json \
  > "runtime/$run.log" 2>&1
sudo docker exec "$nn" hdfs dfs -cat "/snow/warehouse/runs/$run/publication/part-*" > "runtime/$run-package.json"
sudo docker exec "$nn" hdfs fsck "/snow/warehouse/runs/$run" -files -blocks -locations > "runtime/$run-fsck.txt"
printf '%s\n' "$run" > runtime/last-yarn-run.txt
grep '"quality"' "runtime/$run.log" | tail -1
tail -18 "runtime/$run-fsck.txt"
