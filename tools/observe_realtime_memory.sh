set -euo pipefail
test "$(hostname)" = snow-analysis
python3 - <<'PY'
import datetime,json,subprocess

def run(*args):
    return subprocess.run(args,capture_output=True,text=True,check=True).stdout.strip()

rows=[]
for name in ('snow-lab-realtime-kafka-1','snow-lab-realtime-jobmanager-1','snow-lab-realtime-taskmanager-1','snow-lab-analysis-doris-fe-1','snow-lab-analysis-doris-be-1'):
    values=run('sudo','docker','exec',name,'sh','-c','cat /sys/fs/cgroup/memory.current /sys/fs/cgroup/memory.peak /sys/fs/cgroup/memory.max; cat /sys/fs/cgroup/memory.events').splitlines()
    rows.append(dict(name=name,current_bytes=int(values[0]),peak_bytes=int(values[1]),limit_bytes=int(values[2]),events={key:int(value) for key,value in (line.split() for line in values[3:])}))
meminfo=dict(line.split(':',1) for line in open('/proc/meminfo'))
print(json.dumps(dict(node=run('hostname'),measured_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),available_mib=int(meminfo['MemAvailable'].split()[0])//1024,containers=rows,note='cgroup peaks since each container start; TaskManager peak resets on recovery; guest available memory is a snapshot')))
PY
