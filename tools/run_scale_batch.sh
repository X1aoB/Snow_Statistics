#!/usr/bin/env bash
set -euo pipefail
test "$(hostname)" = snow-control
cd /home/snow/Snow_Statistics
size=${1:?Supply 100000 or 1000000}
attempt=${2:?Supply a new lowercase attempt name}
[[ "$attempt" =~ ^[a-z][a-z0-9-]{1,30}$ ]]
case "$size" in 100000) lane=input-100k-v1 ;; 1000000) lane=input-1m-v1 ;; *) exit 2 ;; esac
test ! -e "runtime/scale/$attempt.json"
# Read live replication state on every submission, not a stale landing receipt.
sudo docker exec snow-lab-control-namenode-1 hdfs fsck "/snow/ods/synthetic/scale/$lane" -files -blocks -locations >"runtime/scale/$lane-fsck.txt"
python3 - "$lane" <<'PY'
import re,sys
from pathlib import Path
report=(Path('runtime/scale')/(sys.argv[1]+'-fsck.txt')).read_text()
replicas=[int(x) for x in re.findall(r'Live_repl=(\d+)',report)]
assert len(replicas)==4 and min(replicas)>=2 and 'Status: HEALTHY' in report
PY
python3 - "$size" <<'PY'
import json,sys
from pathlib import Path
golden=json.loads(Path('runtime/scale/golden-01.json').read_bytes())['manifest']
assert golden['master']=='yarn' and golden['quality']==dict(raw=56,valid=28,duplicates=28,quarantined=0,after_cutoff=0)
if int(sys.argv[1])==1000000:
    prior=json.loads(Path('runtime/scale/scale-100k-01.json').read_bytes())['manifest']
    assert prior['master']=='yarn' and prior['quality']==dict(raw=100000,valid=90000,duplicates=10000,quarantined=0,after_cutoff=0)
PY
timeout 900 bash tools/spark_yarn_scale.sh /opt/snow/warehouse/spark/batch.py \
  --input "/snow/ods/synthetic/scale/$lane/*.jsonl.gz" --output /snow/warehouse/scale \
  --run-id "$attempt" --date-from 2026-01-01 --date-to 2026-01-08 --cutoff 2026-01-09T00:00:00Z \
  --expected "/opt/snow/runtime/scale/$lane/expected.json" \
  --package-file "/opt/snow/runtime/scale/$attempt.json" >"runtime/scale/$attempt.log" 2>&1 </dev/null
python3 - "$attempt" "$size" <<'PY'
import json,sys
from pathlib import Path
p=json.loads((Path('runtime/scale')/(sys.argv[1]+'.json')).read_bytes())
n=int(sys.argv[2]); assert p['manifest']['quality']==dict(raw=n,valid=n*9//10,duplicates=n//10,quarantined=0,after_cutoff=0)
assert len(p['daily'])==14
print(json.dumps(p['manifest']))
PY
sudo docker exec snow-lab-control-namenode-1 hdfs fsck "/snow/warehouse/scale/runs/$attempt" -files -blocks -locations >"runtime/scale/$attempt-fsck.txt"
python3 - "$attempt" <<'PY'
import re,sys
from pathlib import Path
report=(Path('runtime/scale')/(sys.argv[1]+'-fsck.txt')).read_text()
replicas=[int(x) for x in re.findall(r'Live_repl=(\d+)',report)]
assert replicas and min(replicas)>=2 and 'Status: HEALTHY' in report
PY
tail -18 "runtime/scale/$attempt-fsck.txt"
