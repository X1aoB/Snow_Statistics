set -euo pipefail
case "$(hostname)" in snow-control|snow-compute|snow-analysis) ;; *) exit 1 ;; esac
python3 - <<'PY'
import datetime,json,subprocess

def run(*args):
    return subprocess.run(args,capture_output=True,text=True,check=True).stdout.strip()
names=run('sudo','docker','ps','--format','{{.Names}}').splitlines()
rows=[]
for name in names:
    if name not in ('snow-spark-yarn','snow-lab-control-namenode-1','snow-lab-control-resourcemanager-1','snow-lab-compute-datanode-1','snow-lab-compute-nodemanager-1','snow-lab-analysis-datanode-1'):
        continue
    try:
        values=run('sudo','docker','exec',name,'sh','-c','cat /sys/fs/cgroup/memory.current /sys/fs/cgroup/memory.peak; cat /sys/fs/cgroup/memory.events').splitlines()
        rows.append(dict(name=name,current_bytes=int(values[0]),peak_bytes=int(values[1]),events=dict(line.split() for line in values[2:])))
    except subprocess.CalledProcessError:
        rows.append(dict(name=name,unavailable=True))
meminfo=dict(line.split(':',1) for line in open('/proc/meminfo'))
print(json.dumps(dict(node=run('hostname'),measured_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),available_mib=int(meminfo['MemAvailable'].split()[0])//1024,containers=rows)))
PY
