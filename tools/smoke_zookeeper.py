"""Quorum, watch, version and ephemeral-session acceptance in the isolated lab."""
import argparse
import json
import re
import socket
import subprocess
import sys
import threading
import time

from ha_lab import Lab
from kazoo.client import KazooClient
from kazoo.exceptions import BadVersionError
from vmware_lab import RUNTIME, VMWARE, guest_ip

from snow_statistics.io import write_json

parser = argparse.ArgumentParser()
parser.add_argument("--lane", required=True)
parser.add_argument("--lease-worker", action="store_true")
parser.add_argument("--host")
args = parser.parse_args()
if not re.fullmatch(r"[a-z][a-z0-9-]{1,24}", args.lane):
    parser.error("New synthetic lane required")
root = "/snow-statistics/" + args.lane


def client(host):
    return KazooClient(hosts=",".join(host + ":" + str(port) for port in (12181, 12182, 12183)),
                       timeout=6, command_retry={"max_tries": 1})


if args.lease_worker:
    assert args.host == guest_ip(VMWARE / "vmrun.exe", RUNTIME / "snow-analysis/snow-analysis.vmx")
    worker = client(args.host)
    worker.start(timeout=20)
    worker.create(root + "/crash-lease", b"ephemeral synthetic lease", ephemeral=True)
    print("lease-created", flush=True)
    while True:
        time.sleep(1)

lab = Lab(args.lane, "zookeeper")
zk, child = None, None
states, observed_watch, history = [], [], []
fired = threading.Event()


def roles():
    result = {}
    for node in (1, 2, 3):
        try:
            with socket.create_connection((lab.host, 12180 + node), timeout=1) as connection:
                connection.sendall(b"srvr")
                message = connection.recv(4096).decode()
            found = re.search(r"Mode: (leader|follower|standalone)", message)
            result[node] = found[1] if found else "not-serving"
        except OSError:
            result[node] = "unavailable"
    return result


def quorum(count):
    result = roles()
    active = [r for r in result.values() if r in ("leader", "follower")]
    return result if len(active) == count and active.count("leader") == 1 else False


try:
    initial = lab.until(lambda: quorum(3), "ZooKeeper quorum of three", 180)
    history.append(dict(phase="initial", roles=initial))
    zk = client(lab.host)
    zk.add_listener(lambda state: states.append(state))
    zk.start(timeout=20)
    zk.ensure_path("/snow-statistics")
    assert zk.exists(root) is None
    zk.create(root, b"synthetic HA acceptance")
    zk.create(root + "/proof", b"before")
    data, version = zk.get(root + "/proof")
    assert data == b"before" and version.version == 0
    lab.observe("before")
    first = next(n for n, role in initial.items() if role == "leader")
    lab.node("stop", "zk", first)
    changed = lab.until(lambda: quorum(2), "ZooKeeper leader re-election")
    history.append(dict(phase="one_down", roles=changed))
    lab.until(lambda: zk.connected, "client reconnects to quorum")

    def watch(event):
        observed_watch.append(dict(type=event.type, state=event.state, path=event.path))
        fired.set()

    assert zk.get(root + "/proof", watch=watch)[0] == b"before"
    version = zk.set_async(root + "/proof", b"one-down", version=0).get(timeout=10)
    assert version.version == 1 and fired.wait(5)
    assert observed_watch[0]["type"] == "CHANGED"
    try:
        zk.set_async(root + "/proof", b"stale-write", version=0).get(timeout=5)
    except BadVersionError:
        pass
    else:
        raise RuntimeError("Stale znode version unexpectedly accepted")
    assert zk.get(root + "/proof")[0] == b"one-down"

    with (lab.folder / "lease-worker.log").open("wb") as log:
        child = subprocess.Popen([sys.executable, __file__, "--lane", args.lane, "--lease-worker", "--host", lab.host],
                                 stdout=log, stderr=subprocess.STDOUT,
                                 creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0)
        lab.until(lambda: zk.exists(root + "/crash-lease"), "separate client ephemeral lease", 30)
        child.terminate()  # Actual abrupt client process death; no graceful session close.
        child.wait(timeout=10)
        lab.until(lambda: zk.exists(root + "/crash-lease") is None, "session expiry removes ephemeral lease", 30)
    zk.stop()
    zk.close()
    zk = None
    second = next(n for n in (1, 2, 3) if n != first)
    remaining = next(n for n in (1, 2, 3) if n not in (first, second))
    lab.node("stop", "zk", second)
    probe = KazooClient(hosts=lab.host + ":" + str(12180 + remaining), timeout=4)
    try:
        try:
            probe.start(timeout=6)
        except probe.handler.timeout_exception as error:
            majority_error = type(error).__name__
        else:
            raise RuntimeError("Minority unexpectedly supplied a read-write session")
    finally:
        probe.stop()
        probe.close()
    write_json(lab.folder / "fault.json", dict(first_stopped=first, second_stopped=second,
               majority_error=majority_error, no_write_submitted_without_session=True))
    lab.node("start", "zk", first)
    lab.node("start", "zk", second)
    restored = lab.until(lambda: quorum(3), "ZooKeeper quorum restored", 180)
    history.append(dict(phase="restored", roles=restored))
    zk = client(lab.host)
    zk.start(timeout=20)
    data, version = zk.get(root + "/proof")
    assert data == b"one-down" and version.version == 1
    assert zk.exists(root + "/crash-lease") is None
    lab.observe("after")
    write_json(lab.folder / "accepted.json", dict(source="synthetic", lane=args.lane,
              zookeeper="3.9.3", kazoo="2.11.0", metadata=history, states=states, watch=observed_watch,
              final_data=data.decode(), final_version=version.version, stale_version_rejected=True,
              abrupt_client_session_expiry_verified=True, majority_error=majority_error,
              no_write_submitted_without_session=True, persistent_data_retained=True,
              scope="Three ZooKeeper processes on one VM/physical host; client session loss is separate from server majority loss"))
    print(json.dumps(dict(leader_changed=changed[first] == "unavailable", watch=True,
                         session_expiry=True, majority_error=majority_error, data_retained=True)), flush=True)
finally:
    if child and child.poll() is None:
        child.terminate()
        child.wait(timeout=10)
    if zk:
        zk.stop()
        zk.close()
    lab.stop()
