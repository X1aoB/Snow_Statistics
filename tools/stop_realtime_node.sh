#!/usr/bin/env bash
set -euo pipefail
test "$(hostname)" = snow-analysis
cd /home/snow/Snow_Statistics
python3 - <<'PY'
import json,time,urllib.request
from pathlib import Path
base='http://127.0.0.1:8081'
def get(path):
    with urllib.request.urlopen(base+path,timeout=5) as response: return json.load(response)
jobs=get('/jobs/overview')['jobs']
active=[j for j in jobs if j['state'] not in ('FINISHED','CANCELED','FAILED')]
assert all(j['name'].startswith('Snow Statistics synthetic ') for j in active)
receipts=[]
for job in active:
    checkpoints=get('/jobs/'+job['jid']+'/checkpoints')
    receipts.append(dict(job_id=job['jid'],name=job['name'],checkpoints=checkpoints))
    request=urllib.request.Request(base+'/jobs/'+job['jid']+'?mode=cancel',method='PATCH')
    urllib.request.urlopen(request,timeout=5).close()
    for _ in range(30):
        if get('/jobs/'+job['jid'])['state']=='CANCELED': break
        time.sleep(1)
    else: raise RuntimeError('Cancellation deadline')
p=Path('runtime/realtime/shutdown-'+str(time.time_ns())+'.json')
p.parent.mkdir(parents=True,exist_ok=True)
p.write_text(json.dumps(receipts,indent=2))
print(p)
PY
sudo docker stop snow-lab-realtime-taskmanager-1 snow-lab-realtime-jobmanager-1 snow-lab-realtime-kafka-1 \
  snow-lab-analysis-doris-fe-1 snow-lab-analysis-doris-be-1 </dev/null
