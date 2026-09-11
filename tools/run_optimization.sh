#!/usr/bin/env bash
set -euo pipefail
test "$(hostname)" = snow-control
cd /home/snow/Snow_Statistics
attempt=${1:?Supply a fresh attempt}
[[ "$attempt" =~ ^[a-z][a-z0-9-]{1,30}$ ]]
test ! -e "runtime/optimization/$attempt"
mkdir -p runtime/optimization
timeout 900 sudo docker exec snow-lab-control-namenode-1 hdfs dfsadmin -safemode wait
sudo docker exec snow-lab-control-namenode-1 hdfs fsck /snow/warehouse/scale/runs/scale-1m-01/dwd -files -blocks -locations >"runtime/optimization/$attempt-input-fsck.txt"
python3 - "$attempt" <<'PY'
import re,sys
from pathlib import Path
s=Path('runtime/optimization/'+sys.argv[1]+'-input-fsck.txt').read_text()
replicas=[int(x) for x in re.findall(r'Live_repl=(\d+)',s)]
assert replicas and min(replicas)>=2 and 'Status: HEALTHY' in s
PY
timeout 900 bash tools/spark_yarn_scale.sh /opt/snow/warehouse/spark/optimize.py --attempt "$attempt" >"runtime/optimization/$attempt.log" 2>&1 </dev/null
test -f "runtime/optimization/$attempt/accepted.json"
cat "runtime/optimization/$attempt/accepted.json"
