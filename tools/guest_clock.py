"""Check VM UTC against the host; optionally align an idle dedicated lab VM.

The host is cross-checked against an uncached HTTPS Date before any correction.
This is a bootstrap/local-lab fallback, not a substitute for precise NTP evidence.
"""
import argparse
import email.utils
import ipaddress
import json
import statistics
import subprocess
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument("--node", required=True, choices=("snow-control", "snow-compute", "snow-analysis"))
parser.add_argument("--ip", required=True, type=ipaddress.ip_address)
parser.add_argument("--set", action="store_true", help="Only when all containers on this dedicated VM are stopped")
args = parser.parse_args()
ssh = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", "-o", "StrictHostKeyChecking=yes",
       "-o", f"HostKeyAlias={args.node}", "-o", f"UserKnownHostsFile={ROOT / 'runtime/vmware/known_hosts'}",
       "-i", str(ROOT / "runtime/vmware/id_ed25519"), f"snow@{args.ip}"]


def remote(command):
    result = subprocess.run([*ssh, command], capture_output=True, text=True, timeout=15)
    if result.returncode:
        raise SystemExit("Guest clock command failed: " + result.stderr.strip())
    return result.stdout.strip()


def offset():
    samples = []
    for _ in range(3):
        start = time.time()
        guest = float(remote("date -u +%s.%N"))
        samples.append(guest - (start + time.time()) / 2)
    return statistics.median(samples)


before = offset()
if args.set:
    # Never step the clock under active database transactions or running jobs.
    if remote("sudo docker ps -q"):
        raise SystemExit("Stop the dedicated VM's containers before changing its clock")
    remote("sudo test -x /usr/sbin/hwclock")
    request = urllib.request.Request(f"https://api.github.com/?snow_clock={time.time_ns()}", method="HEAD",
                                     headers={"User-Agent": "Snow-Statistics-Clock-Check", "Cache-Control": "no-cache"})
    with urllib.request.urlopen(request, timeout=10) as response:
        reference = email.utils.parsedate_to_datetime(response.headers["Date"]).timestamp()
    # HTTP Date is only a coarse UTC sanity check (proxy latency/1-second precision).
    # The strict two-second gate below compares actual host/guest clock samples.
    if abs(time.time() - reference) > 30:
        raise SystemExit("Host and HTTPS reference disagree; refusing to set guest clocks")
    # NTP packets were not arriving in this NAT lab. Use one active discipline:
    # Tools tracks the checked host, and UTC CMOS is written for the next boot.
    remote(f"sudo timedatectl set-ntp false && sudo date -u --set=@{time.time():.6f} >/dev/null "
           "&& sudo hwclock --systohc --utc && sudo vmware-toolbox-cmd timesync enable >/dev/null")
after = offset()
receipt = dict(node=args.node, corrected=args.set, offset_before_seconds=round(before, 3),
               offset_after_seconds=round(after, 3), within_two_seconds=abs(after) < 2,
               reference="host UTC; HTTPS Date checked before correction", measured_epoch=time.time())
(ROOT / f"runtime/clock-{args.node}.json").write_text(json.dumps(receipt, indent=2) + "\n")
print(json.dumps(receipt))
if not receipt["within_two_seconds"]:
    raise SystemExit("Clock skew gate failed; do not measure cross-node freshness")
