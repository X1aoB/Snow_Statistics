"""Create isolated NAT VMware guests with verified Ubuntu image and local SSH trust.

Artifacts, private keys and VM disks stay under ignored runtime/vmware only.
No existing VM or VMware global configuration is changed.
"""
import argparse
import hashlib
import io
import json
import re
import shutil
import subprocess
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LIMITS = json.loads((ROOT / "deploy/resources.json").read_bytes())["local"]
MAX_PROJECT_BYTES = LIMITS["max_project_bytes"]
MIN_HOST_FREE_BYTES = LIMITS["min_host_free_bytes"]
MIN_HOST_AVAILABLE_MIB = LIMITS["min_host_available_mib"]
RUNTIME = ROOT / "runtime/vmware"
VMWARE = Path(r"C:\Program Files (x86)\VMware\VMware Workstation")
BASE_URL = "https://cloud-images.ubuntu.com/releases/noble/release-20260826/"
IMAGE = "ubuntu-24.04-server-cloudimg-amd64.vmdk"
IMAGE_SHA256 = "fb3ba097a9013d759fa13ab22d2b4118bd55452c617ca3758a55303eea96de6e"
NODES = {"snow-control": (6144, 2), "snow-compute": (6144, 4), "snow-analysis": (10240, 4)}
DISK_GB = {"snow-control": 18, "snow-compute": 18, "snow-analysis": 29}
PROFILES = {"batch": {"snow-control": 4096, "snow-compute": 4096, "snow-analysis": 2048},
            "scale": {"snow-control": 2048, "snow-compute": 2048, "snow-analysis": 1024},
            "real-small": {"snow-control": 2048, "snow-compute": 2048, "snow-analysis": 768},
            "hive-only": {"snow-control": 2048, "snow-compute": 1024, "snow-analysis": 1536},
            "ods": {"snow-control": 3584, "snow-compute": 4096, "snow-analysis": 1024},
            "ods-compact": {"snow-control": 3584, "snow-compute": 3072, "snow-analysis": 1024},
            "governance": {"snow-analysis": 2048},
            "ha": {"snow-analysis": 3072},
            "realtime": {"snow-analysis": 4608},
            "olap": {"snow-control": 4096, "snow-analysis": 6144}, "standard": {}}


def configured_memory(vmx):
    match = re.search(r'^memsize\s*=\s*"(\d+)"', vmx.read_text(), re.M)
    if not match:
        raise RuntimeError("VMX has no explicit memory size")
    return int(match[1])


def validate_memory(node, memory):
    # The reduced analysis VM is an explicit, measured small-batch option;
    # it does not lower the minimum for the YARN control/compute nodes.
    if not (1024 <= memory <= NODES[node][0] or node == "snow-analysis" and memory == 768):
        raise RuntimeError("VMX memory exceeds the reviewed node budget")
    return memory


def configure_memory(vmrun, vmx, node, profile):
    # Only our powered-off VMX may change; suspended/running state is not editable.
    running = run(vmrun, "-T", "ws", "list").lower()
    if str(vmx).lower() in running or list(vmx.parent.glob("*.lck")) or list(vmx.parent.glob("*.vmss")):
        raise RuntimeError("Stop the project VM completely before changing its resource profile")
    memory = PROFILES[profile].get(node, NODES[node][0])
    content = vmx.read_text(encoding="utf-8")
    configured_memory(vmx)
    content = re.sub(r'^memsize\s*=\s*"\d+"', f'memsize = "{memory}"', content, count=1, flags=re.M)
    vmx.write_text(content, encoding="utf-8")
    print(json.dumps({"node": node, "profile": profile, "memory_mib": memory}))


def run(*args):
    return subprocess.run([str(a) for a in args], text=True, capture_output=True, check=True).stdout.strip()


def capacity(memory_mb=0):
    free = shutil.disk_usage(ROOT).free
    if free < MIN_HOST_FREE_BYTES:
        raise RuntimeError(f"{MIN_HOST_FREE_BYTES / 1024**3:g} GiB host disk reserve gate failed")
    used = vm_used = 0
    for path in ROOT.rglob("*"):
        try:
            if path.is_file():
                size = path.stat().st_size
                used += size
                if path.is_relative_to(RUNTIME):
                    vm_used += size
        except FileNotFoundError:
            continue  # VMware can rotate transient runtime files during inspection.
    if used + memory_mb * 1024**2 > MAX_PROJECT_BYTES:
        raise RuntimeError(f"{MAX_PROJECT_BYTES / 1024**3:g} GiB project gate failed: files {used / 1024**3:.2f} GiB + reservation {memory_mb / 1024:.2f} GiB")
    if memory_mb:
        available = int(run("powershell", "-NoProfile", "-Command", "(Get-CimInstance Win32_OperatingSystem).FreePhysicalMemory")) // 1024
        if available < memory_mb + MIN_HOST_AVAILABLE_MIB:
            raise RuntimeError(f"Free RAM {available} MiB below reservation {memory_mb} MiB + {MIN_HOST_AVAILABLE_MIB} MiB host reserve")
    return {"free_disk_gib": round(free / 1024**3, 2), "project_files_gib": round(used / 1024**3, 2),
            "vm_files_gib": round(vm_used / 1024**3, 2), "project_limit_gib": MAX_PROJECT_BYTES / 1024**3,
            "min_host_free_disk_gib": MIN_HOST_FREE_BYTES / 1024**3}


def guest_ip(vmrun, vmx):
    try:
        return run(vmrun, "-T", "ws", "getGuestIPAddress", vmx)
    except subprocess.CalledProcessError:
        # cloud-init precedes open-vm-tools; fall back to this VM's own NAT lease.
        # SSH still must use the seeded host key and StrictHostKeyChecking=yes.
        match = re.search(r'ethernet0.generatedAddress = "([0-9a-f:]+)"', vmx.read_text(), re.I)
        if not match:
            raise RuntimeError("VM has no generated MAC yet; boot it first") from None
        leases = Path(r"C:\ProgramData\VMware\vmnetdhcp.leases").read_text()
        blocks = re.findall(r"lease ([0-9.]+)\s*\{([^}]+)\}", leases)
        found = [ip for ip, body in blocks if f"hardware ethernet {match[1].lower()};" in body.lower()]
        if not found:
            raise RuntimeError("No NAT lease yet; allow cloud-init to finish") from None
        return found[-1]


def download():
    RUNTIME.mkdir(parents=True, exist_ok=True)
    sums = urllib.request.urlopen(BASE_URL + "SHA256SUMS", timeout=30).read().decode()
    expected = next(line.split()[0] for line in sums.splitlines() if line.endswith(IMAGE))
    if expected != IMAGE_SHA256:
        raise RuntimeError("Pinned Ubuntu release checksum changed; review before updating lock")
    target = RUNTIME / IMAGE
    if not target.exists():
        print("Downloading pinned Ubuntu 24.04 image (~566 MiB)", flush=True)
        with urllib.request.urlopen(BASE_URL + IMAGE, timeout=60) as response, target.with_suffix(".download").open("wb") as stream:
            shutil.copyfileobj(response, stream)
        target.with_suffix(".download").replace(target)
    with target.open("rb") as stream:
        actual = hashlib.file_digest(stream, "sha256").hexdigest()
    if actual != expected:
        raise RuntimeError("Ubuntu image SHA256 mismatch")
    (RUNTIME / "image.lock.json").write_text(json.dumps({"url": BASE_URL + IMAGE, "sha256": actual}, indent=2))
    return target


def keypair(path):
    if not path.exists():
        run("ssh-keygen", "-t", "ed25519", "-N", "", "-C", "snow-statistics-local-lab", "-f", path)


def prepare():
    import pycdlib
    print(json.dumps(capacity()), flush=True)
    image = download()
    client_key = RUNTIME / "id_ed25519"
    keypair(client_key)
    known = []
    for name, (ram, cores) in NODES.items():
        directory = RUNTIME / name
        directory.mkdir(exist_ok=True)
        host_key = directory / "ssh_host_ed25519_key"
        keypair(host_key)
        known.append(name + " " + host_key.with_suffix(".pub").read_text().strip())
        userdata = {"hostname": name, "manage_etc_hosts": True, "ssh_pwauth": False, "disable_root": True,
                    "users": [{"name": "snow", "groups": "sudo", "shell": "/bin/bash", "sudo": "ALL=(ALL) NOPASSWD:ALL",
                               "lock_passwd": True, "ssh_authorized_keys": [client_key.with_suffix(".pub").read_text().strip()]}],
                    "ssh_keys": {"ed25519_private": host_key.read_text(), "ed25519_public": host_key.with_suffix(".pub").read_text()},
                    "package_update": False,
                    "runcmd": [["systemctl", "enable", "--now", "ssh"]]}
        iso = directory / "seed.iso"
        if not iso.exists():
            disk = pycdlib.PyCdlib()
            disk.new(interchange_level=3, joliet=3, vol_ident="cidata")
            for filename, text in {"user-data": "#cloud-config\n" + json.dumps(userdata),
                                    "meta-data": f"instance-id: {name}-v1\nlocal-hostname: {name}\n"}.items():
                data = text.encode()
                disk.add_fp(io.BytesIO(data), len(data), iso_path="/" + filename.upper().replace("-", "_") + ";1", joliet_path="/" + filename)
            disk.write(str(iso))
            disk.close()
        vmdk = directory / "system.vmdk"
        if not vmdk.exists():
            run(VMWARE / "vmware-vdiskmanager.exe", "-r", image, "-t", "0", vmdk)
            run(VMWARE / "vmware-vdiskmanager.exe", "-x", f"{DISK_GB[name]}GB", vmdk)
        vmx = directory / f"{name}.vmx"
        if not vmx.exists():
            config = {".encoding": "UTF-8", "config.version": "8", "virtualHW.version": "20", "displayName": name,
                      "guestOS": "ubuntu-64", "memsize": str(ram), "numvcpus": str(cores),
                      "scsi0.present": "TRUE", "scsi0.virtualDev": "lsilogic", "scsi0:0.present": "TRUE", "scsi0:0.fileName": "system.vmdk",
                      "ide1:0.present": "TRUE", "ide1:0.deviceType": "cdrom-image", "ide1:0.fileName": "seed.iso",
                      "pciBridge0.present": "TRUE", "ethernet0.present": "TRUE", "ethernet0.connectionType": "nat", "ethernet0.virtualDev": "e1000",
                      "ethernet0.addressType": "generated", "uuid.action": "create", "msg.autoAnswer": "TRUE",
                      "tools.syncTime": "TRUE", "mks.enable3d": "FALSE", "floppy0.present": "FALSE"}
            vmx.write_text("\n".join(f'{k} = "{v}"' for k, v in config.items()) + "\n", encoding="utf-8")
        print(f"Prepared {name}: {ram} MiB, {cores} vCPU", flush=True)
    (RUNTIME / "known_hosts").write_text("\n".join(known) + "\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["prepare", "configure", "start", "stop", "ip", "status"])
    parser.add_argument("--node", choices=list(NODES), default="snow-control")
    parser.add_argument("--profile", choices=list(PROFILES), default="batch")
    parser.add_argument("--reserve-mib", type=int, default=0,
                        help="Additional headroom for start/status; does not change the host/project limits")
    args = parser.parse_args()
    if not 0 <= args.reserve_mib <= 4096 or args.reserve_mib and args.action not in {"start", "status"}:
        parser.error("--reserve-mib is 0..4096 and only applies to start/status")
    vmrun = VMWARE / "vmrun.exe"
    vmx = RUNTIME / args.node / f"{args.node}.vmx"
    if args.action == "prepare":
        prepare()
    elif args.action == "status":
        print(json.dumps(capacity(args.reserve_mib)))
        print(run(vmrun, "-T", "ws", "list"))
    elif args.action == "start":
        memory = validate_memory(args.node, configured_memory(vmx))
        print(json.dumps(capacity(memory + args.reserve_mib)), flush=True)
        print(run(vmrun, "-T", "ws", "start", vmx, "nogui"))
    elif args.action == "configure":
        configure_memory(vmrun, vmx, args.node, args.profile)
    elif args.action == "stop":
        print(run(vmrun, "-T", "ws", "stop", vmx, "soft"))
    else:
        print(guest_ip(vmrun, vmx))


if __name__ == "__main__":
    main()
