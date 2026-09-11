#!/usr/bin/env bash
set -euo pipefail
cd /home/snow/Snow_Statistics
set -a
. lab/locks/images.env
set +a
.venv/bin/python tools/prepare_spark_fixture.py
sudo docker pull --quiet "$SPARK_IMAGE" > /tmp/snow-spark-pull.log 2>&1
run="smoke-$(date -u +%Y%m%dT%H%M%S)"
mkdir -p runtime/spark-output
sudo docker run --rm --name snow-spark-smoke --memory 3g --cpus 2 \
  --user 0:0 -v "$PWD:/opt/snow" -w /tmp "$SPARK_IMAGE" \
  /opt/spark/bin/spark-submit --master 'local[2]' --driver-memory 1g \
  /opt/snow/warehouse/spark/batch.py --input /opt/snow/runtime/spark-fixture/events.jsonl \
  --output /opt/snow/runtime/spark-output --run-id "$run" \
  --date-from 2026-01-01 --date-to 2026-01-04 --cutoff 2026-01-05T00:00:00Z \
  > runtime/spark-smoke.log 2>&1
grep '"quality"' runtime/spark-smoke.log | tail -1
sudo docker run --rm --name snow-spark-ops-smoke --memory 3g --cpus 2 \
  --user 0:0 -v "$PWD:/opt/snow" -w /tmp "$SPARK_IMAGE" \
  /opt/spark/bin/spark-submit --master 'local[2]' --driver-memory 1g \
  /opt/snow/warehouse/spark/ops.py --input /opt/snow/runtime/spark-fixture/ops.jsonl \
  --output "/opt/snow/runtime/spark-output/ops-$run" --as-of 2026-01-04 \
  > runtime/spark-ops-smoke.log 2>&1
echo "Spark event and operations smoke completed: $run"
