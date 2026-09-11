#!/usr/bin/env bash
set -euo pipefail
test "$(hostname)" = snow-control
cd /home/snow/Snow_Statistics
lane=${1:?Supply the synthetic replay lane}
attempt=${2:?Supply a new correction attempt, for example corrected01}
[[ "$lane" =~ ^[a-z0-9_]{1,24}$ ]]
[[ "$attempt" =~ ^[a-z][a-z0-9]{0,31}$ ]]
test -z "$(sudo docker ps -q)"
. lab/locks/images.env
directory="runtime/realtime/$lane"
test -f "$directory/correction-events.jsonl"
test -f "$directory/correction-expected.json"
test ! -e "$directory/spark-correction.json"
cutoff=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["cutoff"])' "$directory/correction-input.json")
sudo docker run --rm --name snow-realtime-correction --memory 3g --cpus 2 --user 0:0 \
  -v "$PWD:/opt/snow" -w /tmp "$SPARK_IMAGE" /opt/spark/bin/spark-submit --master 'local[2]' --driver-memory 1g \
  /opt/snow/warehouse/spark/batch.py --input "/opt/snow/$directory/correction-events.jsonl" \
  --output "/opt/snow/$directory/spark-output" --run-id "realtime-$lane-$attempt" \
  --date-from 2026-01-01 --date-to 2026-01-02 --cutoff "$cutoff" \
  --expected "/opt/snow/$directory/correction-expected.json" \
  --package-file "/opt/snow/$directory/spark-correction.json" >"$directory/spark.log" 2>&1 </dev/null
python3 -c 'import json,sys; print(json.dumps(json.load(open(sys.argv[1]))))' "$directory/spark-correction.json"
