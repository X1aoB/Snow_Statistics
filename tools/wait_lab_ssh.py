"""Bounded read-only boot readiness; never retries a mutation or weakens SSH trust."""
import argparse
import subprocess
import time

from vmware_lab import NODES, RUNTIME, VMWARE, guest_ip

parser = argparse.ArgumentParser()
parser.add_argument("--node", choices=NODES, required=True)
args = parser.parse_args()
host = guest_ip(VMWARE / "vmrun.exe", RUNTIME / args.node / (args.node + ".vmx"))
ssh = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=3", "-o", "StrictHostKeyChecking=yes",
       "-o", "HostKeyAlias=" + args.node, "-o", f"UserKnownHostsFile={RUNTIME / 'known_hosts'}",
       "-i", str(RUNTIME / "id_ed25519"), "snow@" + host]
deadline = time.monotonic() + 90
while time.monotonic() < deadline:
    try:
        result = subprocess.run([*ssh, "sudo docker info --format '{{.ServerVersion}}'"],
                                capture_output=True, text=True, timeout=8)
        if result.returncode == 0:
            print(args.node + " verified SSH/Docker ready: " + result.stdout.strip())
            break
    except subprocess.TimeoutExpired:
        pass
    time.sleep(1)
else:
    raise SystemExit("Boot readiness deadline; no services were started")
