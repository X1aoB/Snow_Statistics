set -euo pipefail
test "$(hostname)" = snow-analysis
python3 - <<'PY'
import datetime,json,re,subprocess
def run(*args): return subprocess.run(args,capture_output=True,text=True,check=True).stdout.strip()
names=run('sudo','docker','ps','--filter','label=com.docker.compose.project=snow-lab-ha','--format','{{.Names}}').splitlines()
assert all(re.fullmatch(r'snow-lab-ha-(?:broker|zk)[123]-1',n) for n in names)
rows=[]
for name in names:
    x=run('sudo','docker','exec',name,'sh','-c','cat /sys/fs/cgroup/memory.current /sys/fs/cgroup/memory.peak /sys/fs/cgroup/memory.max; cat /sys/fs/cgroup/memory.events').splitlines()
    rows.append(dict(name=name,current_bytes=int(x[0]),peak_bytes=int(x[1]),limit_bytes=int(x[2]),events={k:int(v) for k,v in (s.split() for s in x[3:])}))
meminfo=dict(s.split(':',1) for s in open('/proc/meminfo'))
print(json.dumps(dict(node='snow-analysis',measured_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),available_mib=int(meminfo['MemAvailable'].split()[0])//1024,containers=rows,note='Each container peak is since its most recent start; no claim of simultaneous RSS')))
PY
