#!/usr/bin/env bash
set -euo pipefail
test "$(hostname)" = snow-control
cd /home/snow/Snow_Statistics
attempt=${1:?Supply accepted lake attempt}
[[ "$attempt" =~ ^[a-z][a-z0-9-]{1,30}$ ]]
test -f "runtime/lake/$attempt/accepted.json"
test -z "$(sudo docker ps -q --filter name=^snow-spark-yarn$)"
test ! -e "runtime/lake/$attempt/namenode-restart.json"
trap 'sudo docker start snow-lab-control-namenode-1 >/dev/null </dev/null || true' EXIT
sudo docker stop snow-lab-control-namenode-1 </dev/null
sudo python3 - "$attempt" <<'PY'
import json,sys,urllib.request,urllib.error
from pathlib import Path
try:
    urllib.request.urlopen('http://127.0.0.1:9870/',timeout=3)
except (urllib.error.URLError,TimeoutError):
    Path('runtime/lake/'+sys.argv[1]+'/namenode-unavailable.json').write_text(json.dumps(dict(http_unavailable_while_stopped=True)))
else:
    raise RuntimeError('Stopped NameNode unexpectedly reachable')
PY
sudo docker start snow-lab-control-namenode-1 </dev/null
python3 - <<'PY'
import socket,time,xml.etree.ElementTree as ET
properties={p.findtext('name'):p.findtext('value') for p in ET.parse('lab/generated/hadoop/hdfs-site.xml').getroot()}
host,port=properties['dfs.namenode.rpc-address'].rsplit(':',1)
for _ in range(60):
    try:
        with socket.create_connection((host,int(port)),timeout=1): pass
        break
    except OSError:
        time.sleep(1)
else:
    raise RuntimeError('NameNode RPC readiness deadline')
PY
timeout 120 sudo docker exec snow-lab-control-namenode-1 hdfs dfsadmin -safemode wait
sudo docker exec snow-lab-control-namenode-1 hdfs fsck "/snow/lake/$attempt" -files -blocks -locations | sudo tee "runtime/lake/$attempt/fsck.txt" >/dev/null
sudo python3 - "$attempt" <<'PY'
import hashlib,json,re,sys
from pathlib import Path
root=Path('runtime/lake')/sys.argv[1]
report=(root/'fsck.txt').read_text()
replicas=[int(x) for x in re.findall(r'Live_repl=(\d+)',report)]
assert replicas and min(replicas)>=2 and 'Status: HEALTHY' in report
(root/'namenode-restart.json').write_text(json.dumps(dict(restarted=True,healthy=True,minimum_live_replicas=min(replicas),nonempty_blocks=len(replicas),fsck_sha256=hashlib.sha256((root/'fsck.txt').read_bytes()).hexdigest(),note='Controlled single NameNode restart, no HA or fencing claim')))
print((root/'namenode-restart.json').read_text())
PY
