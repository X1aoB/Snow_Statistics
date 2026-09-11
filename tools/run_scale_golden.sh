#!/usr/bin/env bash
# Golden prerequisite for an already bootstrapped laboratory; never replace a receipt.
set -euo pipefail
test "$(hostname)" = snow-control
cd /home/snow/Snow_Statistics
attempt=${1:-scale-golden-01}
[[ "$attempt" =~ ^[a-z][a-z0-9-]{1,30}$ ]]
test ! -e runtime/scale/golden-01.json
.venv/bin/python tools/prepare_spark_fixture.py
nn=snow-lab-control-namenode-1
target=/snow/ods/synthetic/events/fixture.jsonl
timeout 90 sudo docker exec "$nn" hdfs dfsadmin -safemode wait
local_hash=$(sha256sum runtime/spark-fixture/events.jsonl | cut -d' ' -f1)
remote_hash=$(sudo docker exec "$nn" hdfs dfs -cat "$target" | sha256sum | cut -d' ' -f1)
test "$local_hash" = "$remote_hash"
mkdir -p runtime/scale
timeout 600 bash tools/spark_yarn_scale.sh /opt/snow/warehouse/spark/batch.py \
  --input "$target" --output /snow/warehouse --run-id "$attempt" \
  --date-from 2026-01-01 --date-to 2026-01-04 --cutoff 2026-01-05T00:00:00Z \
  --expected /opt/snow/runtime/spark-fixture/expected.json \
  --package-file /opt/snow/runtime/scale/golden-01.json >runtime/scale/golden-01.log 2>&1 </dev/null
cat runtime/scale/golden-01.json
