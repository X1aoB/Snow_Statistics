#!/usr/bin/env bash
set -euo pipefail
test "$(hostname)" = snow-control
cd /home/snow/Snow_Statistics
size=${1:?Supply 100000 or 1000000}
case "$size" in 100000) lane=input-100k-v1 ;; 1000000) lane=input-1m-v1 ;; *) exit 2 ;; esac
python3 - "$lane" "$size" <<'PY'
import hashlib,json,sys
from pathlib import Path
p=Path('runtime/scale')/sys.argv[1]; m=json.loads((p/'manifest.json').read_bytes())
assert m['source']=='synthetic' and m['events']==int(sys.argv[2]) and len(m['shards'])==4
for i, row in enumerate(m['shards']):
    assert row['name']=='events-%02d.jsonl.gz'%i
    f=p/row['name']; assert f.stat().st_size==row['bytes'] and hashlib.sha256(f.read_bytes()).hexdigest()==row['sha256']
PY
nn=snow-lab-control-namenode-1
target="/snow/ods/synthetic/scale/$lane"
timeout 90 sudo docker exec "$nn" hdfs dfsadmin -safemode wait
sudo docker exec "$nn" hdfs dfs -mkdir -p "$target"
for file in "runtime/scale/$lane"/*.jsonl.gz; do
  name=$(basename "$file")
  if ! sudo docker exec "$nn" hdfs dfs -test -e "$target/$name"; then
    sudo docker cp "$file" "$nn:/tmp/snow-scale-$name"
    sudo docker exec "$nn" hdfs dfs -put "/tmp/snow-scale-$name" "$target/$name"
  fi
  local_hash=$(sha256sum "$file" | cut -d' ' -f1)
  remote_hash=$(sudo docker exec "$nn" hdfs dfs -cat "$target/$name" | sha256sum | cut -d' ' -f1)
  test "$local_hash" = "$remote_hash"
done
timeout 120 sudo docker exec "$nn" hdfs dfs -setrep -w 2 "$target"
sudo docker exec "$nn" hdfs fsck "$target" -files -blocks -locations >"runtime/scale/$lane-fsck.txt"
tail -18 "runtime/scale/$lane-fsck.txt"
