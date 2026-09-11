#!/usr/bin/env bash
set -euo pipefail
test "$(hostname)" = snow-control
cd /home/snow/Snow_Statistics
attempt=${1:?Supply a new lake attempt}
[[ "$attempt" =~ ^[a-z][a-z0-9-]{1,30}$ ]]
verify=${2:-}
test -z "$verify" || test "$verify" = --verify
python3 - <<'PY'
import hashlib,json
from pathlib import Path
name='iceberg-spark-runtime-3.5_2.12-1.10.0.jar'
lock=json.loads(Path('lab/locks/jars.json').read_bytes())[name]
p=Path('runtime/jars')/name
assert p.stat().st_size == lock['bytes'] and hashlib.sha256(p.read_bytes()).hexdigest()==lock['sha256']
PY
set -a
. lab/.env
set +a
mkdir -p runtime/lake
suffix=create
if test -n "$verify"; then suffix=verify; fi
timeout 900 bash tools/spark_yarn_scale.sh \
  --jars /opt/snow/runtime/jars/iceberg-spark-runtime-3.5_2.12-1.10.0.jar \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.snow=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.snow.type=hadoop \
  --conf "spark.sql.catalog.snow.warehouse=hdfs://$CONTROL_IP:9000/snow/lake/$attempt" \
  /opt/snow/warehouse/spark/iceberg_dwd.py --attempt "$attempt" ${verify:+"$verify"} >"runtime/lake/$attempt-$suffix.log" 2>&1 </dev/null
tail -8 "runtime/lake/$attempt-$suffix.log"
