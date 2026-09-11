"""Review sparse-disk compaction including VMware's temporary full-disk copy."""
import argparse
import json
import shutil
import subprocess

from vmware_lab import DISK_GB, NODES, ROOT, RUNTIME, VMWARE, capacity, run

parser = argparse.ArgumentParser()
parser.add_argument("--node", choices=NODES, required=True)
parser.add_argument("--execute", action="store_true")
args = parser.parse_args()
directory = (RUNTIME / args.node).resolve()
disk = (directory / "system.vmdk").resolve()
assert disk.is_relative_to(RUNTIME.resolve()) and disk.parent == directory
snapshot = capacity()
running = run(VMWARE / "vmrun.exe", "-T", "ws", "list").lower()
powered_off = str(directory / (args.node + ".vmx")).lower() not in running and not list(directory.glob("*.lck")) and not list(directory.glob("*.vmss"))
# Reserve the configured logical capacity plus working overhead, even if the
# source sparse file is currently smaller. This is intentionally conservative.
copy_bytes = max(disk.stat().st_size, DISK_GB[args.node] * 1024**3) + 1024**3
project_bytes = sum(p.stat().st_size for p in ROOT.rglob("*") if p.is_file())
free = shutil.disk_usage(ROOT).free
eligible = powered_off and project_bytes + copy_bytes <= 60 * 1024**3 and free - copy_bytes >= 35 * 1024**3
print(json.dumps(dict(node=args.node, current=snapshot, temporary_copy_gib=round(copy_bytes / 1024**3, 2),
                     powered_off=powered_off, eligible=eligible,
                     scope="Only this project's system.vmdk; no files or history are deleted")), flush=True)
if args.execute:
    if not eligible:
        raise SystemExit("Compaction refused: account for the temporary VMDK copy and keep both resource reserves")
    subprocess.run([str(VMWARE / "vmware-vdiskmanager.exe"), "-k", str(disk)], check=True)
