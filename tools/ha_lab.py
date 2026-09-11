"""Scoped host-side helpers for one synthetic HA VM; no shared-host cleanup."""
import json
import re
import subprocess
import time

from vmware_lab import (
    MAX_PROJECT_BYTES,
    MIN_HOST_AVAILABLE_MIB,
    ROOT,
    RUNTIME,
    VMWARE,
    capacity,
    guest_ip,
    run,
)

from snow_statistics.io import write_json


class Lab:
    def __init__(self, lane, profile):
        if not re.fullmatch(r"[a-z][a-z0-9-]{1,24}", lane) or profile not in ("kafka", "zookeeper"):
            raise ValueError("Invalid isolated experiment")
        self.folder = ROOT / "runtime/ha" / (profile + "-" + lane)
        self.folder.mkdir(parents=True, exist_ok=False)
        self.host = guest_ip(VMWARE / "vmrun.exe", RUNTIME / "snow-analysis/snow-analysis.vmx")
        running = run(VMWARE / "vmrun.exe", "-T", "ws", "list")
        assert "Total running VMs: 1" in running and "snow-analysis.vmx" in running
        self.ssh = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", "-o", "StrictHostKeyChecking=yes",
                    "-o", "HostKeyAlias=snow-analysis", "-o", f"UserKnownHostsFile={RUNTIME / 'known_hosts'}",
                    "-i", str(RUNTIME / "id_ed25519"), "snow@" + self.host]
        self.samples, self.last_check = [], 0
        write_json(self.folder / "preflight.json", capacity(1024))

    def remote(self, command, timeout=45):
        return subprocess.check_output([*self.ssh, command], text=True, timeout=timeout)

    def check(self):
        if time.monotonic() - self.last_check < 5:
            return
        row = capacity()
        row["host_available_mib"] = int(run("powershell", "-NoProfile", "-Command", "(Get-CimInstance Win32_OperatingSystem).FreePhysicalMemory")) // 1024
        row["measured_epoch"] = time.time()
        self.samples.append(row)
        self.last_check = time.monotonic()
        write_json(self.folder / "resources.json", self.samples)
        if row["project_files_gib"] >= MAX_PROJECT_BYTES / 1024**3 - .25 or row["host_available_mib"] < MIN_HOST_AVAILABLE_MIB:
            raise RuntimeError("Resource early stop; preserve all state")

    def until(self, condition, label, seconds=90):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            self.check()
            value = condition()
            if value:
                return value
            time.sleep(1)
        raise RuntimeError(label + " deadline")

    def node(self, action, kind, number):
        assert action in ("stop", "start") and kind in ("broker", "zk") and number in (1, 2, 3)
        return self.remote(f"sudo docker {action} snow-lab-ha-{kind}{number}-1")

    def observe(self, label):
        script = (ROOT / "tools/observe_ha_memory.sh").read_text().replace("\r\n", "\n").encode()
        output = subprocess.check_output([*self.ssh, "bash -se"], input=script, timeout=45)
        write_json(self.folder / ("memory-" + label + ".json"), json.loads(output))

    def stop(self):
        names = self.remote("sudo docker ps --filter label=com.docker.compose.project=snow-lab-ha --format '{{.Names}}'").splitlines()
        assert all(re.fullmatch(r"snow-lab-ha-(?:broker|zk)[123]-1", n) for n in names)
        if names:
            self.remote("sudo docker stop " + " ".join(names), timeout=60)
        write_json(self.folder / "shutdown.json", dict(services_stopped=True, resources=capacity(),
                                                      note="VM remains available for the next isolated phase; volumes retained"))
