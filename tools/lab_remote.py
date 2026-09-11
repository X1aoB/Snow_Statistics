"""SSH/SCP to this project's VMs only, using seeded host keys, never passwords."""
import argparse
import subprocess
from pathlib import Path

from vmware_lab import NODES, RUNTIME, VMWARE, guest_ip


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", choices=NODES, required=True)
    parser.add_argument("--script", type=Path, help="Send a local shell script through stdin")
    parser.add_argument("--upload", type=Path)
    parser.add_argument("--download", type=Path)
    parser.add_argument("--remote", help="Remote file path for SCP")
    args = parser.parse_args()
    if sum(bool(v) for v in (args.script, args.upload, args.download)) != 1:
        parser.error("Choose exactly one of --script, --upload, --download")
    ip = guest_ip(VMWARE / "vmrun.exe", RUNTIME / args.node / f"{args.node}.vmx")
    options = ["-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "StrictHostKeyChecking=yes",
               "-o", f"HostKeyAlias={args.node}", "-o", f"UserKnownHostsFile={RUNTIME / 'known_hosts'}",
               "-i", str(RUNTIME / "id_ed25519")]
    if args.script:
        result = subprocess.run(["ssh", *options, f"snow@{ip}", "bash -se"],
                                input=args.script.read_text(encoding="utf-8-sig").replace("\r\n", "\n").encode())
    else:
        if not args.remote or not args.remote.startswith("/home/snow/"):
            parser.error("SCP paths must be explicit paths under /home/snow/")
        remote = f"snow@{ip}:{args.remote}"
        paths = [str(args.upload), remote] if args.upload else [remote, str(args.download)]
        result = subprocess.run(["scp", *options, *paths])
    raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()
