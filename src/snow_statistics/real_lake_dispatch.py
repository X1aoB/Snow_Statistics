"""Fixed-node orchestration and actual Spark processes for the lake authority.

No reverse SSH, new credentials, registry replication or arbitrary receipt CLI.
All cross-node traffic uses the existing Windows -> VM pinned-host transport.
"""
import json
import os
import shlex
import signal
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

from .io import atomic_write, digest, write_json
from .lifecycle import timestamp
from .publication import canonical, publication_lock
from .real_lab import REMOTE_ROOT, Runner
from .real_lake_authority import (
    accept,
    checked_engine_receipt,
    cleanup_copies,
    engine_identity,
    location,
    read,
    read_attempt,
    reserve,
    validate_descriptor,
)


def _clean_driver(root, directory, name):
    """Read/stop only the exact unique container; never enumerate or prune shared Docker."""
    result = subprocess.run(["sudo", "docker", "inspect", name], cwd=root, capture_output=True, timeout=20)
    if result.returncode == 0:
        value = json.loads(result.stdout)[0]
        created_id = read(directory / "data/driver.cid", 128).decode().strip()
        if (len(created_id) != 64 or any(char not in "0123456789abcdef" for char in created_id)
                or value["Id"] != created_id or value["Name"] != "/" + name
                or value["Config"]["Labels"].get("org.snow-statistics.lake-driver") != name):
            raise ValueError("Lake driver ownership changed; refusing container mutation")
        subprocess.run(["sudo", "docker", "rm", "-f", value["Id"]], cwd=root,
                       stdout=subprocess.DEVNULL, check=True, timeout=40)
    elif b"No such" not in result.stderr:
        raise RuntimeError("Cannot verify absence of the exact lake driver")
    after = subprocess.run(["sudo", "docker", "inspect", name], cwd=root, capture_output=True, timeout=20)
    if after.returncode == 0 or b"No such" not in after.stderr:
        raise RuntimeError("Exact lake driver absence was not confirmed; retain its CID")
    (directory / "data/driver.cid").unlink(missing_ok=True)


def process_identity(pid):
    """PID plus Linux start ticks and exact argv; a reused PID is not our worker."""
    if type(pid) is not int or not 1 <= pid <= 2147483647:
        raise ValueError("A bounded exact Linux process ID is required")
    try:
        value = Path("/proc") / str(pid)
        fields = (value / "stat").read_text().rsplit(")", 1)[1].split()
        if fields[0] == "Z":
            return None
        return dict(pid=pid, start_ticks=fields[19], argv_sha256=digest((value / "cmdline").read_bytes()))
    except FileNotFoundError:
        return None


@contextmanager
def node_worker(root, config, run_id, attempt, phase):
    directory = location(root, config, run_id, attempt)
    with publication_lock(directory / "worker-admission"):
        if (directory / "cancelled.json").exists():
            raise ValueError("This lake attempt was cancelled; use a fresh attempt")
        record = directory / "node-worker.json"
        if record.exists():
            previous = json.loads(read(record, 65536))
            if process_identity(previous["process"]["pid"]) == previous["process"]:
                raise ValueError("This lake attempt already has a live exact worker")
        identity = process_identity(os.getpid())
        if identity is None:
            raise ValueError("Cannot bind the Linux worker process identity")
        write_json(record, dict(phase=phase, process=identity, config_sha256=digest(canonical(config))))
    yield


def cancel_driver(root, config, run_id, attempt):
    """No aggregate read or lifetime renewal; cancellation also works after expiry."""
    directory = location(root, config, run_id, attempt)
    with publication_lock(directory / "worker-admission"):
        write_json(directory / "cancelled.json", dict(cancelled_at=datetime.now(UTC).isoformat()))
        target = directory / "node-worker.json"
        worker = json.loads(read(target, 65536)) if target.exists() else None
        if worker and (worker["phase"] not in {"execute", "verify"} or worker["config_sha256"] != digest(canonical(config))):
            raise ValueError("Cancellation worker ownership changed")
    if worker and process_identity(worker["process"]["pid"]) == worker["process"]:
        os.kill(worker["process"]["pid"], signal.SIGTERM)
        deadline = time.monotonic() + 125
        while process_identity(worker["process"]["pid"]) == worker["process"]:
            if time.monotonic() >= deadline:
                os.kill(worker["process"]["pid"], signal.SIGKILL)
                break
            time.sleep(0.25)
    descriptor_path = directory / "descriptor.json"
    if descriptor_path.exists():
        descriptor = json.loads(read(descriptor_path, 65536))
        # Validate the original issued identity, not a renewed read admission.
        validate_descriptor(descriptor, config, run_id, attempt, now=timestamp(descriptor["issued_at"]))
        _clean_driver(root, directory, "snow-real-lake-" + digest(canonical(descriptor))[:20])
    elif (directory / "data/driver.cid").exists():
        raise ValueError("Unidentified driver CID requires explicit review")
    if worker and process_identity(worker["process"]["pid"]) == worker["process"]:
        raise ValueError("Exact node worker remains live after cancellation")
    return dict(status="driver_cancelled", data_deleted=False)


def run_spark(root, directory, descriptor, mode):
    """One finite process group and a capped retained log; cleanup is part of success."""
    root = Path(root)
    if mode not in {"execute", "verify"} or descriptor["engine"] != engine_identity(root, verify_jar=True):
        raise ValueError("Fixed lake execution artifacts changed")
    remaining = (timestamp(descriptor["read_until"]) - datetime.now(UTC)).total_seconds()
    if remaining < 160:
        raise ValueError("Not enough live admission time to launch a bounded lake job")
    relative = directory.relative_to(root).as_posix()
    prefix = "/opt/snow/" + relative + "/data/"
    name = "snow-real-lake-" + digest(canonical(descriptor))[:20]
    command = ["bash", "tools/real_lake_spark.sh", relative + "/data/driver.cid", name, descriptor["scope"],
               "/opt/snow/warehouse/spark/real_iceberg" + ("_verify" if mode == "verify" else "") + ".py",
               "--input", prefix + "input.json", "--input-sha256", descriptor["input_sha256"],
               "--receipt", prefix + ("verified-engine.json" if mode == "verify" else "receipt.json")]
    if mode == "verify":
        command += ["--execution", prefix + "receipt.json", "--execution-sha256", digest(read(directory / "data/receipt.json", 65536)),
                    "--read-until", descriptor["read_until"]]
    log = directory / "data" / ("verify.log" if mode == "verify" else "spark.log")
    # The node operation first requires no unfinished execution marker. A
    # pre-existing exact name is never presumed to belong to this invocation.
    exists = subprocess.run(["sudo", "docker", "inspect", name], cwd=root, capture_output=True, timeout=20)
    if exists.returncode == 0 or b"No such" not in exists.stderr:
        raise ValueError("Existing or unknown driver state blocks a new launch")
    counts, log_errors = {"bytes": 0, "retained_bytes": 0}, []
    with log.open("wb") as output:
        process, worker = None, None
        def drain():
            try:
                while chunk := process.stdout.read(65536):
                    counts["bytes"] += len(chunk)
                    kept = chunk[:max(0, 1024**2 - counts["retained_bytes"])]
                    output.write(kept)
                    counts["retained_bytes"] += len(kept)
            except Exception as error:
                log_errors.append(error)

        try:
            process = subprocess.Popen(command, cwd=root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                       stdin=subprocess.DEVNULL, start_new_session=True)
            worker = threading.Thread(target=drain, daemon=True)
            worker.start()
            budget = (timestamp(descriptor["read_until"]) - datetime.now(UTC)).total_seconds() - 150
            if budget <= 0:
                raise ValueError("Lake admission has no remaining bounded cleanup margin")
            code = process.wait(timeout=min(600, budget))
            if code:
                raise RuntimeError("Lake Spark application failed; inspect its bounded private log")
        finally:
            try:
                if process is not None and process.poll() is None:
                    os.killpg(process.pid, signal.SIGTERM)
                    try:
                        process.wait(timeout=35)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait(timeout=10)
            finally:
                # A failed process-group signal must not skip exact-container
                # cleanup. Both errors remain failures, never successful runs.
                try:
                    if process is not None:
                        _clean_driver(root, directory, name)
                finally:
                    if worker is not None and worker.ident is not None:
                        worker.join(timeout=10)
                        if worker.is_alive():
                            raise RuntimeError("Lake driver log pipe did not close")
                    if process is not None:
                        process.stdout.close()
        if log_errors:
            raise RuntimeError("Lake driver bounded log could not be retained") from log_errors[0]
    return counts | {"truncated": counts["bytes"] > counts["retained_bytes"]}


def execute(root, config, run_id, attempt):
    descriptor, value, local = read_attempt(root, config, run_id, attempt)
    directory = location(root, config, run_id, attempt)
    with publication_lock(directory):
        marker = directory / "execution.json"
        if marker.exists():
            previous = json.loads(read(marker, 65536))
            if previous.get("status") != "complete" or previous.get("descriptor_sha256") != digest(canonical(descriptor)):
                raise ValueError("An interrupted lake attempt cannot overwrite existing tables; use a new attempt")
            receipt = checked_engine_receipt(json.loads(read(local.readable("receipt.json", datetime.now(UTC)))), value["bundle"], now=datetime.now(UTC))
            if digest(canonical(receipt)) != previous["receipt_sha256"]:
                raise ValueError("Completed lake receipt changed")
            return previous
        write_json(marker, dict(status="started", descriptor_sha256=digest(canonical(descriptor)), started_at=datetime.now(UTC).isoformat()))
        log = run_spark(root, directory, descriptor, "execute")
        read_attempt(root, config, run_id, attempt)  # Also refuses a result completed after the short read cutoff.
        receipt = checked_engine_receipt(json.loads(read(local.readable("receipt.json", datetime.now(UTC)), 65536)), value["bundle"], now=datetime.now(UTC))
        result = dict(status="complete", descriptor_sha256=digest(canonical(descriptor)),
                      receipt_sha256=digest(canonical(receipt)), completed_at=datetime.now(UTC).isoformat(), log=log)
        write_json(marker, result)
        return result


def verify(root, config, run_id, attempt):
    descriptor, value, local = read_attempt(root, config, run_id, attempt)
    directory = location(root, config, run_id, attempt)
    with publication_lock(directory):
        marker = json.loads(read(directory / "execution.json", 65536))
        execution = checked_engine_receipt(json.loads(read(local.readable("receipt.json", datetime.now(UTC)), 65536)), value["bundle"], now=datetime.now(UTC))
        if (marker.get("status") != "complete" or marker.get("descriptor_sha256") != digest(canonical(descriptor))
                or marker.get("receipt_sha256") != digest(canonical(execution))):
            raise ValueError("Only this coordinator's completed execution can be verified")
        # Existing verify.json never substitutes for running the actual reader.
        run_spark(root, directory, descriptor, "verify")
        read_attempt(root, config, run_id, attempt)
        actual = checked_engine_receipt(json.loads(read(local.readable("verified-engine.json", datetime.now(UTC)), 65536)), value["bundle"], now=datetime.now(UTC))
        if actual["tables"] != execution["tables"] or actual["application_id"] == execution["application_id"]:
            raise ValueError("Independent read-only Spark verification disagrees with execution")
        proof = dict(schema_version=1, source="real", kind="iceberg_actual_readback",
                     descriptor_sha256=digest(canonical(descriptor)), input_sha256=descriptor["input_sha256"],
                     receipt_sha256=digest(canonical(execution)), execution=execution, verification=actual,
                     verified_at=datetime.now(UTC).isoformat())
        write_json(local.path("verify.json"), proof)
        return dict(verified=True, evidence_sha256=digest(read(local.path("verify.json"), 65536)))


class LakeRunner(Runner):
    """Single public entry: no user-provided success file or backend command."""
    def run(self, command, *, timeout=1200, **unused):
        options = (dict(creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW)
                   if os.name == "nt" else dict(start_new_session=True))
        process = subprocess.Popen(command, cwd=self.root, stdin=subprocess.DEVNULL, **options)
        try:
            process.wait(timeout=timeout)
            if process.returncode:
                raise RuntimeError("Owned lake transport failed; recover its exact attempt")
        finally:
            if process.poll() is None:
                if os.name == "nt":
                    subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"], check=True,
                                   capture_output=True, timeout=20, creationflags=subprocess.CREATE_NO_WINDOW)
                else:
                    os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=10)

    def phase(self, node, phase, run_id, attempt, evidence_sha=None):
        if node == "snow-analysis" and phase in {"prepare", "confirm"}:
            from .real_hive_dispatch import HiveCoordinator
            return HiveCoordinator(self.config, self.config_file, self.root).run(
                "lake-" + phase, run_id, attempt=attempt, evidence_sha256=evidence_sha)
        arguments = [".venv/bin/python", "tools/real_lake_authority.py", "--config", self.config_file,
                     "--run-id", run_id, "--attempt", attempt, "--node-phase", phase]
        if evidence_sha:
            arguments += ["--evidence-sha256", evidence_sha]
        identity = digest(canonical([self.config_file, run_id, attempt]))[:20]
        filename = self.root / "runtime/real/operator" / (node + "-lake-" + phase + "-" + identity + ".sh")
        atomic_write(filename, ("set -euo pipefail\numask 077\ncd " + REMOTE_ROOT + "\n" + shlex.join(arguments) + "\n").encode())
        command = [sys.executable, "tools/lab_remote.py", "--node", node, "--script", str(filename)]
        if phase != "cancel-driver":
            command += ["--reserve-mib", "256"]
        self.run(command, timeout=850 if phase != "cancel-driver" else 240)

    def copy(self, node, local, relative, *, upload=False):
        self.run([sys.executable, "tools/lab_remote.py", "--node", node,
                  "--upload" if upload else "--download", str(local), "--remote", REMOTE_ROOT + "/" + relative], timeout=60)

    def stage(self, node, phase, run_id, attempt):
        arguments = [".venv/bin/python", "tools/real_lake_stage.py", "--config", self.config_file,
                     "--run-id", run_id, "--attempt", attempt, "--stage", phase]
        identity = digest(canonical([self.config_file, run_id, attempt]))[:20]
        filename = self.root / "runtime/real/operator" / (node + "-lake-stage-" + phase + "-" + identity + ".sh")
        atomic_write(filename, ("set -euo pipefail\numask 077\ncd " + REMOTE_ROOT + "\n" + shlex.join(arguments) + "\n").encode())
        command = [sys.executable, "tools/lab_remote.py", "--node", node, "--script", str(filename)]
        # Recovery must remain reachable after disk/RAM headroom deteriorates.
        # It does not start new compute; the node still checks exact ownership.
        if phase != "restore":
            command += ["--reserve-mib", "256"]
        self.run(command, timeout=240)

    def restore_stages(self, run_id, attempt, nodes=("snow-compute", "snow-control", "snow-analysis")):
        failures = []
        for node in nodes:
            try:
                self.stage(node, "restore", run_id, attempt)
            except Exception as error:
                failures.append(error)
        if failures:
            raise RuntimeError("Lake stage recovery incomplete; preserve state and rerun --recover-stages") from failures[0]

    def recover(self, run_id, attempt, nodes=("snow-compute", "snow-control", "snow-analysis")):
        try:
            self.phase("snow-control", "cancel-driver", run_id, attempt)
        finally:
            self.restore_stages(run_id, attempt, nodes)

    def lake(self, run_id, attempt):
        with publication_lock(self.root / "runtime/real/lake-operator"):
            return self._coordinated_lake(run_id, attempt)

    def _coordinated_lake(self, run_id, attempt):
        cleanup_copies(self.root)
        reserved, complete = [], False
        try:
            for node in ("snow-analysis", "snow-control", "snow-compute"):
                reserved.append(node)
                self.stage(node, "reserve", run_id, attempt)
            result = self._lake(run_id, attempt)
            complete = True
            return result
        finally:
            nodes = tuple(node for node in ("snow-compute", "snow-control", "snow-analysis") if node in reserved)
            if complete:
                self.restore_stages(run_id, attempt, nodes)
            else:
                self.recover(run_id, attempt, nodes)

    def _lake(self, run_id, attempt):
        directory = location(self.root, self.config, run_id, attempt)
        relative = directory.relative_to(self.root).as_posix()
        directory.mkdir(parents=True, exist_ok=True)
        self.phase("snow-analysis", "prepare", run_id, attempt)
        incoming = directory / "incoming-descriptor.json"
        self.copy("snow-analysis", incoming, relative + "/descriptor.json")
        descriptor = validate_descriptor(json.loads(read(incoming, 65536)), self.config, run_id, attempt)
        local = reserve(self.root, self.config, run_id, attempt, descriptor)
        self.copy("snow-analysis", local.path("package.json.tmp"), relative + "/data/package.json")
        accept(self.root, self.config, run_id, attempt)
        self.phase("snow-control", "directories", run_id, attempt)
        self.copy("snow-control", directory / "descriptor.json", relative + "/incoming-descriptor.json", upload=True)
        self.phase("snow-control", "reserve", run_id, attempt)
        self.copy("snow-control", local.path("package.json"), relative + "/data/package.json.tmp", upload=True)
        self.phase("snow-control", "accept", run_id, attempt)
        self.stage("snow-control", "yarn", run_id, attempt)
        self.stage("snow-compute", "yarn", run_id, attempt)
        self.phase("snow-control", "execute", run_id, attempt)
        self.phase("snow-control", "verify", run_id, attempt)  # This must actually run, including on retry.
        self.copy("snow-control", local.path("verify.json"), relative + "/data/verify.json")
        # Pin the exact bytes obtained through our just-completed trusted SSH operation.
        proof = read(local.readable("verify.json", datetime.now(UTC)), 65536)
        self.copy("snow-analysis", local.path("verify.json"), relative + "/data/verify.json", upload=True)
        # The same coordinator holds the exact proof across the resource-phase
        # switch. It does not accept an operator-provided result file.
        self.restore_stages(run_id, attempt)
        self.phase("snow-analysis", "confirm", run_id, attempt, digest(proof))
        self.copy("snow-analysis", directory / "confirmed.json", relative + "/confirmed.json")
        return json.loads(read(directory / "confirmed.json", 65536))
