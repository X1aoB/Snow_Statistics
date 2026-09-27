"""Finite Windows -> analysis/control metadata RPC for actual Hive cleanup.

The analysis process retains registry locks and all authority. Windows relays a
request to an actual authenticated control SSH process, never a supplied success
file. Neither VM obtains another VM's private key. This module manages only its
own processes and one request-labelled temporary Spark container.
"""
import importlib.util
import json
import os
import queue
import re
import secrets
import shlex
import signal
import socket
import subprocess
import sys
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

from .docker_absence import inspect_missing
from .io import atomic_write, digest, write_json
from .lifecycle import timestamp
from .publication import canonical, publication_lock
from .real_hive import catalog_runner_context
from .real_hive_contract import validate_request
from .real_lab import REMOTE_ROOT, private_relative, read_json, secret_file, validate_config

MAX_FRAME = 12 * 1024**2
# Local worst-case cleanup is 60s for the process group, 10s for two pipes
# and 55s for exact Docker stop/readback. Keep an additional 25s margin.
WORKER_CLEANUP_SECONDS = 150
OPERATIONS = {"register", "verify", "cleanup", "catalog-cleanup", "permit", "view", "lake-prepare", "lake-confirm"}
ENGINE_FILES = (
    "src/snow_statistics/real_hive.py", "src/snow_statistics/real_hive_contract.py",
    "src/snow_statistics/real_hive_spark.py", "src/snow_statistics/real_hive_dispatch.py",
    "src/snow_statistics/docker_absence.py",
    "tools/real_hive_dispatch.py", "tools/spark_hive_catalog.sh",
    "warehouse/spark/real_hive_catalog.py", "lab/spark-submit-locked.sh",
    "lab/locks/images.env", "lab/locks/hive-client.sha256", "lab/locks/spark-jars.sha256",
)


def _keys(value, fields):
    if type(value) is not dict or set(value) != set(fields):
        raise ValueError("Unexpected Hive dispatch fields")


def _hash(value):
    if not isinstance(value, str) or not re.fullmatch(r"[a-f0-9]{64}", value):
        raise ValueError("Invalid Hive dispatch checksum")
    return value


def operation_arguments(operation, run_id, attempt, evidence_sha256):
    if operation not in OPERATIONS:
        raise ValueError("Unknown bounded Hive operation")
    needs_run = operation in {"register", "verify", "permit", "view", "lake-prepare", "lake-confirm"}
    if needs_run != bool(run_id) or run_id and not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", run_id):
        raise ValueError("Unexpected Hive run identifier")
    lake = operation.startswith("lake-")
    if lake != bool(attempt) or attempt and not re.fullmatch(r"[A-Za-z0-9_-]{1,60}", attempt):
        raise ValueError("Unexpected lake attempt identifier")
    if bool(evidence_sha256) != (operation == "lake-confirm"):
        raise ValueError("Only an actual lake confirmation carries its coordinator's evidence hash")
    if evidence_sha256:
        _hash(evidence_sha256)


def engine_identity(root):
    result = {}
    for name in ENGINE_FILES:
        path = Path(root) / name
        if path.resolve() != path.absolute() or not path.is_file() or path.stat().st_size > 2 * 1024**2:
            raise ValueError("Catalog execution source is absent, linked or unbounded")
        result[name] = digest(path.read_bytes())
    return result


def frame_bytes(value):
    body = canonical(value)
    if len(body) > MAX_FRAME:
        raise ValueError("Hive metadata frame exceeds its bound")
    return body + b"\n"


def read_frame(stream):
    body = stream.readline(MAX_FRAME + 2)
    if not body or len(body) > MAX_FRAME + 1 or not body.endswith(b"\n"):
        raise ValueError("Hive RPC ended or produced an unbounded/incomplete frame")
    value = json.loads(body)
    if frame_bytes(value) != body:
        raise ValueError("Hive RPC metadata is not canonical")
    return value


class FrameReader:
    """Bounded reader thread lets the coordinator enforce a finite session."""
    def __init__(self, stream):
        self.queue = queue.Queue(maxsize=2)
        self.closed = threading.Event()
        self.ended = threading.Event()
        def read():
            while not self.closed.is_set():
                try:
                    item = read_frame(stream)
                except BaseException as error:
                    item = error
                try:
                    self.queue.put(item, timeout=1)
                except queue.Full:
                    self.closed.set()
                    self.ended.set()
                if isinstance(item, BaseException):
                    self.ended.set()
                    return
        self.thread = threading.Thread(target=read, daemon=True)
        self.thread.start()

    def next(self, timeout):
        if self.closed.is_set() or timeout <= 0:
            raise ValueError("Hive session is closed or expired")
        try:
            value = self.queue.get(timeout=timeout)
        except queue.Empty as error:
            raise TimeoutError("Hive RPC timed out") from error
        if isinstance(value, BaseException):
            raise ValueError("Hive RPC transport closed") from value
        return value


def validate_envelope(value, *, now=None, cancellation=False):
    _keys(value, ("schema_version", "kind", "session", "sequence", "lane", "request", "request_sha256",
                  "engine", "authority_sha256", "expires_at"))
    if (type(value["schema_version"]) is not int or value["schema_version"] != 1 or value["kind"] != "catalog" or
            type(value["sequence"]) is not int or not 1 <= value["sequence"] <= 1000 or
            not isinstance(value["lane"], str) or not re.fullmatch(r"[a-z][a-z0-9-]{2,23}", value["lane"])):
        raise ValueError("Invalid Hive session scope")
    _hash(value["session"])
    _hash(value["authority_sha256"])
    _hash(value["request_sha256"])
    _keys(value["engine"], ENGINE_FILES)
    for expected in value["engine"].values():
        _hash(expected)
    current = now or datetime.now(UTC)
    # Cancellation validates the old reservation's shape but never reauthorizes
    # a read. Its exact on-disk request and Docker ID remain mandatory below.
    validate_request(value["request"], now=timestamp(value["request"]["requested_at"]) if cancellation else current)
    if digest(canonical(value["request"])) != value["request_sha256"]:
        raise ValueError("Hive request checksum differs")
    deadline = timestamp(value["expires_at"])
    lower = timestamp(value["request"]["requested_at"]) if cancellation else current
    if not lower < deadline <= timestamp(value["request"]["requested_at"]) + timedelta(minutes=10):
        raise ValueError("Hive RPC permission expired or widened")
    for entry in value["request"]["tables"].values():
        if "/snow/warehouse/real/" + value["lane"] + "/" not in entry["descriptor"]["location"]:
            raise ValueError("Hive table escaped this dispatch lane")
    return value


def validate_reply(value, request):
    _keys(value, ("schema_version", "kind", "session", "sequence", "request_sha256", "engine", "result"))
    if (value["schema_version"] != 1 or type(value["schema_version"]) is not int or value["kind"] != "catalog-result" or
            any(value[key] != request[key] for key in ("session", "sequence", "request_sha256", "engine"))):
        raise ValueError("Hive reply belongs to another live request")
    result = value["result"]
    _keys(result, ("schema_version", "source", "action", "tables", "request_sha256", "engine", "master",
                   "application_id", "metastore_uri", "checked_at"))
    spec = request["request"]
    if (result["schema_version"] != 1 or result["source"] != "real" or result["action"] != spec["action"] or
            result["request_sha256"] != request["request_sha256"] or result["engine"] != "Spark 3.5.7" or
            result["master"] != "local[1]" or result["metastore_uri"] != spec["metastore_uri"] or
            not re.fullmatch(r"local-[0-9]+", result["application_id"]) or
            not timestamp(spec["requested_at"]) <= timestamp(result["checked_at"]) < timestamp(request["expires_at"]) or
            type(result["tables"]) is not dict or set(result["tables"]) != set(spec["tables"])):
        raise ValueError("Actual catalog result scope or execution identity differs")
    for row in result["tables"].values():
        if spec["action"] == "cleanup":
            _keys(row, ("state",))
            if row["state"] not in {"absent", "present"}:
                raise ValueError("Unexpected catalog cleanup result")
        else:
            _keys(row, ("state", "rows", "rows_sha256"))
            if row["state"] != "present" or type(row["rows"]) is not int or not 0 <= row["rows"] <= 10000:
                raise ValueError("Unexpected catalog aggregate result")
            _hash(row["rows_sha256"])
    return result


def execution_budget(envelope, *, now=None):
    current = now or datetime.now(UTC)
    validate_envelope(envelope, now=current)
    remaining = (timestamp(envelope["expires_at"]) - current).total_seconds()
    if remaining <= WORKER_CLEANUP_SECONDS + 15:
        raise ValueError("Insufficient live permission for Hive startup and exact cleanup")
    return min(420, remaining - WORKER_CLEANUP_SECONDS)


class AuthorityRunner:
    """Used only inside the analysis process; all registry reads remain local."""
    def __init__(self, root, config, incoming, outgoing, session, deadline):
        self.root, self.config = Path(root), config
        self.incoming, self.outgoing = incoming, outgoing
        self.session, self.deadline = session, deadline
        self.sequence, self.closed = 0, False
        self.lock = threading.Lock()

    def __call__(self, command, *, cwd, timeout, **unused):
        from .real_hive import HiveRegistry
        from .real_remote_lifecycle import RealRemoteLifecycle
        with self.lock:
            if self.closed or datetime.now(UTC) >= self.deadline:
                raise ValueError("Hive coordinator is no longer live")
            if len(command) != 4 or command[:2] != ["bash", "tools/spark_hive_catalog.sh"] or Path(cwd) != self.root:
                raise ValueError("Hive bridge accepts only the fixed catalog worker")
            path = self.root / command[2]
            expected = f"runtime/real/lifecycle/{self.config['lane']}/hive-requests/{_hash(command[3])}.json"
            if command[2] != expected or path.resolve() != path.absolute() or path.stat().st_size > 8 * 1024**2:
                raise ValueError("Hive request escaped the authoritative metadata path")
            body = path.read_bytes()
            request = json.loads(body)
            if canonical(request) != body or digest(body) != command[3]:
                raise ValueError("Authoritative Hive request changed")
            manager = RealRemoteLifecycle(path.parent.parent)
            registry = HiveRegistry(manager)
            def scope():
                owner, state = manager._read()
                return digest(canonical([owner, state, registry.read()]))
            self.sequence += 1
            envelope = dict(schema_version=1, kind="catalog", session=self.session, sequence=self.sequence,
                            lane=self.config["lane"], request=request, request_sha256=command[3],
                            engine=engine_identity(self.root), authority_sha256=scope(),
                            expires_at=min(self.deadline, timestamp(request["requested_at"]) + timedelta(minutes=10)).isoformat())
            validate_envelope(envelope)
            self.outgoing.write(frame_bytes(envelope))
            self.outgoing.flush()
            try:
                reply = self.incoming.next(min(timeout, (timestamp(envelope["expires_at"]) - datetime.now(UTC)).total_seconds()))
                result = validate_reply(reply, envelope)
                validate_envelope(envelope)
                if scope() != envelope["authority_sha256"] or engine_identity(self.root) != envelope["engine"]:
                    raise ValueError("Authority or catalog execution source changed during RPC")
                return subprocess.CompletedProcess(command, 0, "SNOW_HIVE_RESULT=" + json.dumps(result) + "\n", "")
            except BaseException:
                self.closed = True
                raise


def _controlled_path(root, relative):
    path = Path(root) / relative
    if path.resolve() != path.absolute():
        raise ValueError("Hive metadata path traverses a link")
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    return path


def _worker_record(root, envelope):
    return _controlled_path(root, f"runtime/real/hive-dispatch/{envelope['session']}/{envelope['sequence']}/worker.json")


def stop_worker(root, envelope, *, run=subprocess.run):
    """Stop only a previously reserved request's exact labelled container ID."""
    validate_envelope(envelope, cancellation=True)
    record = _worker_record(root, envelope)
    if not record.exists():
        return
    value = json.loads(record.read_bytes())
    if value != {"request": envelope, "name": "snow-real-hive-" + envelope["request_sha256"][:20]}:
        raise ValueError("Hive worker cleanup ownership changed")
    name = value["name"]
    relative = f"runtime/real/lifecycle/{envelope['lane']}/hive-requests/{envelope['request_sha256']}.cid"
    cidfile = _controlled_path(root, relative)
    query = ["sudo", "docker", "inspect", "--format",
             '{{.Id}}|{{.Name}}|{{.State.Running}}|{{index .Config.Labels "snow.hive.request"}}', name]
    result = run(query, capture_output=True, timeout=15, cwd=root)
    if result.returncode:
        if not inspect_missing(result, name, formatted=True):
            raise ValueError("Cannot verify exact Hive worker absence")
        return
    if not cidfile.is_file() or cidfile.stat().st_size > 66:
        raise ValueError("Hive worker lacks its exact Docker creation identity")
    cid = cidfile.read_text().strip()
    if not re.fullmatch(r"[a-f0-9]{64}", cid):
        raise ValueError("Invalid Hive worker container identity")
    expected = f"{cid}|/{name}|true|{envelope['request_sha256']}\n".encode()
    stopped = expected.replace(b"|true|", b"|false|")
    if result.stdout not in {expected, stopped}:
        raise ValueError("Refusing to stop a replaced Hive worker")
    if result.stdout == expected:
        run(["sudo", "docker", "stop", "-t", "10", cid], check=True, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL, timeout=25, cwd=root)
    final = run(query, capture_output=True, timeout=15, cwd=root)
    if ((final.returncode == 0 and final.stdout != stopped)
            or (final.returncode != 0 and not inspect_missing(final, name, formatted=True))):
        raise ValueError("Owned Hive worker did not stop")


def bounded_catalog_worker(command, *, cwd, timeout, **unused):
    """Finite local driver with bounded output and owned process-group cleanup."""
    process = subprocess.Popen(command, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               stdin=subprocess.DEVNULL, start_new_session=True)
    stdout, excessive, drain_errors = bytearray(), threading.Event(), []
    def drain(stream, limit, retained=None):
        size = 0
        try:
            while body := stream.read(65536):
                size += len(body)
                if size > limit:
                    excessive.set()
                if retained is not None:
                    retained.extend(body[:max(0, limit - len(retained))])
        except Exception as error:
            drain_errors.append(error)
            excessive.set()
    readers = [threading.Thread(target=drain, args=(process.stdout, MAX_FRAME, stdout), daemon=True),
               threading.Thread(target=drain, args=(process.stderr, 256 * 1024), daemon=True)]
    for reader in readers:
        reader.start()
    deadline = time.monotonic() + timeout
    try:
        while process.poll() is None and not excessive.is_set() and time.monotonic() < deadline:
            excessive.wait(0.1)
        if process.poll() is None:
            raise TimeoutError("Hive driver exceeded its duration/output budget")
        if process.returncode:
            raise RuntimeError("Actual Hive driver failed")
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=50)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=10)
        for reader in readers:
            reader.join(timeout=5)
        for stream in (process.stdout, process.stderr):
            stream.close()
    if excessive.is_set() or drain_errors or any(reader.is_alive() for reader in readers):
        raise ValueError("Hive driver produced unbounded or incomplete output")
    return subprocess.CompletedProcess(command, 0, stdout.decode("utf-8"), "")


def run_control(envelope, root, *, runner=None, stop=stop_worker):
    """Input authorizes metadata work, never supplies an execution result."""
    validate_envelope(envelope)
    if envelope["engine"] != engine_identity(root):
        raise ValueError("Control catalog sources differ from analysis")
    values = {}
    for line in (Path(root) / "lab/.env").read_text().splitlines():
        if line.startswith("CONTROL_IP="):
            values["CONTROL_IP"] = line.split("=", 1)[1]
    if envelope["request"]["metastore_uri"] != "thrift://" + values.get("CONTROL_IP", "") + ":9083":
        raise ValueError("Catalog target differs from the fixed control metastore")
    execution_budget(envelope)
    from .real_lake_authority import cleanup_copies
    cleanup_copies(root)
    record = _worker_record(root, envelope)
    relative = f"runtime/real/lifecycle/{envelope['lane']}/hive-requests/{envelope['request_sha256']}.json"
    path = _controlled_path(root, relative)
    with publication_lock(record.parent):
        if record.exists() or path.exists() or path.with_suffix(".cid").exists():
            raise ValueError("A Hive request cannot adopt or replay an existing execution")
        write_json(record, dict(request=envelope, name="snow-real-hive-" + envelope["request_sha256"][:20]))
        atomic_write(path, canonical(envelope["request"]))
        try:
            result = (runner or bounded_catalog_worker)(["bash", "tools/spark_hive_catalog.sh", relative, envelope["request_sha256"]],
                                                   cwd=root, timeout=execution_budget(envelope), capture_output=True, text=True, check=True)
            if result.returncode != 0 or len(result.stdout.encode()) > MAX_FRAME:
                raise ValueError("Actual catalog worker failed or exceeded its output limit")
            lines = [line[len("SNOW_HIVE_RESULT="):] for line in result.stdout.splitlines() if line.startswith("SNOW_HIVE_RESULT=")]
            if len(lines) != 1:
                raise ValueError("Actual catalog execution did not return exactly one result")
            reply = dict(schema_version=1, kind="catalog-result", session=envelope["session"], sequence=envelope["sequence"],
                         request_sha256=envelope["request_sha256"], engine=envelope["engine"], result=json.loads(lines[0]))
            validate_reply(reply, envelope)
            validate_envelope(envelope)
            if envelope["engine"] != engine_identity(root):
                raise ValueError("Catalog sources changed during execution")
            return reply
        finally:
            stop(root, envelope)
            if path.exists():
                if path.resolve() != path.absolute() or digest(path.read_bytes()) != envelope["request_sha256"]:
                    raise ValueError("Owned request metadata changed before exact cleanup")
                path.unlink()


class NodeTransport:
    """Only fixed internal Python phases through the existing pinned-host SSH."""
    def __init__(self, root):
        self.root = Path(root)
        spec = importlib.util.spec_from_file_location("snow_hive_vmware", self.root / "tools/vmware_lab.py")
        self.vmware = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.vmware)

    def start(self, node, arguments, *, view=False, reserve=True):
        if os.name != "nt" or node not in {"snow-analysis", "snow-control"}:
            raise ValueError("The catalog coordinator uses Windows pinned-host VM transport")
        if reserve:
            self.vmware.capacity(256)
        elif node != "snow-control" or arguments != ["--node-phase", "cancel"]:
            raise ValueError("Only exact worker cancellation may bypass startup capacity admission")
        runtime = self.vmware.RUNTIME
        ip = self.vmware.guest_ip(self.vmware.VMWARE / "vmrun.exe", runtime / node / (node + ".vmx"))
        options = ["-T", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "-o", "HostKeyAlias=" + node,
                   "-o", "UserKnownHostsFile=" + str(runtime / "known_hosts"), "-o", "ConnectTimeout=10",
                   "-o", "ServerAliveInterval=10", "-o", "ServerAliveCountMax=2", "-o", "ExitOnForwardFailure=yes",
                   "-i", str(runtime / "id_ed25519")]
        if view:
            if node != "snow-analysis":
                raise ValueError("Only the private analysis view has a loopback port")
            options += ["-L", "127.0.0.1:8502:127.0.0.1:8501"]
        command = shlex.join([REMOTE_ROOT + "/.venv/bin/python", "-u", REMOTE_ROOT + "/tools/real_hive_dispatch.py", *arguments])
        return subprocess.Popen(["ssh", *options, "snow@" + ip, command], cwd=self.root,
                                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                creationflags=subprocess.CREATE_NO_WINDOW)

    def worker(self, envelope, *, cancel=False):
        if not cancel:
            execution_budget(envelope)
        process = self.start("snow-control", ["--node-phase", "cancel" if cancel else "worker"], reserve=not cancel)
        try:
            # The control process reserves 150 seconds for its process-group
            # and Docker shutdown. Wait beyond that bounded local cleanup but
            # still leave time to cancel the exact worker before the cutoff.
            if not cancel:
                execution_budget(envelope)
            timeout = 70 if cancel else min(570, (timestamp(envelope["expires_at"]) - datetime.now(UTC)).total_seconds() - 15)
            output, _ = process.communicate(frame_bytes(envelope), timeout=timeout)
            if process.returncode or len(output) > MAX_FRAME + 1:
                raise RuntimeError("Actual control Hive process failed")
            import io
            stream = io.BytesIO(output)
            value = read_frame(stream)
            if stream.read():
                raise ValueError("Control returned extra protocol frames")
            return value
        finally:
            terminate(process)


def terminate(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)


class HiveCoordinator:
    def __init__(self, config, config_file, root, *, transport=None):
        self.config, self.root = validate_config(config), Path(root)
        private_relative(config_file, "config", ".json")
        self.config_file = config_file
        self.transport = transport or NodeTransport(root)

    def run(self, operation, run_id=None, *, attempt=None, evidence_sha256=None, duration_seconds=900):
        operation_arguments(operation, run_id, attempt, evidence_sha256)
        if type(duration_seconds) is not int or not 60 <= duration_seconds <= 1800:
            raise ValueError("Use a finite 60..1800 second catalog session")
        from .real_lake_authority import cleanup_copies
        cleanup_copies(self.root)
        args = ["--node-phase", "authority", "--config", self.config_file, "--operation", operation,
                "--duration-seconds", str(duration_seconds)]
        for key, value in (("--run-id", run_id), ("--attempt", attempt), ("--evidence-sha256", evidence_sha256)):
            if value:
                args += [key, value]
        process = self.transport.start("snow-analysis", args, view=operation == "view")
        reader, pending, sequence = FrameReader(process.stdout), None, 0
        deadline = datetime.now(UTC) + timedelta(seconds=duration_seconds)
        try:
            ready = reader.next(30)
            _keys(ready, ("schema_version", "kind", "session", "operation", "expires_at"))
            if ready["schema_version"] != 1 or ready["kind"] != "ready" or ready["operation"] != operation:
                raise ValueError("Analysis did not open the requested catalog session")
            _hash(ready["session"])
            deadline = timestamp(ready["expires_at"])
            if not datetime.now(UTC) < deadline <= datetime.now(UTC) + timedelta(seconds=duration_seconds):
                raise ValueError("Analysis session deadline is expired or exceeds its duration")
            while True:
                frame = reader.next((deadline - datetime.now(UTC)).total_seconds())
                if frame.get("kind") == "complete":
                    _keys(frame, ("schema_version", "kind", "session", "operation", "result"))
                    if frame["session"] != ready["session"] or frame["operation"] != operation or frame["schema_version"] != 1:
                        raise ValueError("Hive completion belongs to another session")
                    process.wait(timeout=15)
                    if process.returncode:
                        raise RuntimeError("Analysis did not complete catalog authority checks")
                    return frame["result"]
                validate_envelope(frame)
                sequence += 1
                if (frame["session"] != ready["session"] or frame["sequence"] != sequence or
                        frame["lane"] != self.config["lane"] or timestamp(frame["expires_at"]) > deadline or
                        frame["request"]["metastore_uri"] != "thrift://" + self.config["nodes"]["snow-control"] + ":9083"):
                    raise ValueError("Hive request escaped its live coordinator session")
                pending = frame
                reply = self.transport.worker(frame)
                validate_reply(reply, frame)
                validate_envelope(frame)
                process.stdin.write(frame_bytes(reply))
                process.stdin.flush()
                pending = None
        finally:
            reader.closed.set()
            try:
                terminate(process)
            finally:
                try:
                    if pending is not None:
                        self.transport.worker(pending, cancel=True)
                finally:
                    for stream in (process.stdin, process.stdout):
                        if not stream.closed:
                            stream.close()


def authority(root, config_file, operation, run_id, attempt, evidence_sha256, duration_seconds, incoming, outgoing):
    """Only the analysis node calls this; public input cannot provide a receipt."""
    import contextlib
    root = Path(root)
    config = validate_config(read_json(secret_file(root, config_file)))
    operation_arguments(operation, run_id, attempt, evidence_sha256)
    if type(duration_seconds) is not int or not 60 <= duration_seconds <= 1800:
        raise ValueError("Use a finite 60..1800 second catalog session")
    from .real_lake_authority import cleanup_copies
    cleanup_copies(root)
    session, deadline = secrets.token_hex(32), datetime.now(UTC) + timedelta(seconds=duration_seconds)
    reader = FrameReader(incoming)
    finished = threading.Event()
    adapter = AuthorityRunner(root, config, reader, outgoing, session, deadline)
    outgoing.write(frame_bytes(dict(schema_version=1, kind="ready", session=session, operation=operation, expires_at=deadline.isoformat())))
    outgoing.flush()
    timer = threading.Timer(duration_seconds, lambda: os.kill(os.getpid(), signal.SIGTERM))
    timer.daemon = True
    timer.start()
    def disconnect_watchdog():
        while not finished.wait(1):
            if reader.ended.is_set():
                adapter.closed = True
                os.kill(os.getpid(), signal.SIGTERM)
                return
    threading.Thread(target=disconnect_watchdog, daemon=True).start()
    try:
        with publication_lock(root / "runtime/real/lifecycle" / config["lane"] / "hive-dispatch-session"):
            with catalog_runner_context(adapter), contextlib.redirect_stdout(sys.stderr):
                if operation.startswith("lake-"):
                    from .real_lake_authority import confirm, prepare
                    result = (prepare(root, config, run_id, attempt) if operation == "lake-prepare" else
                              confirm(root, config, run_id, attempt, evidence_sha256))
                elif operation == "view":
                    from streamlit.web import bootstrap
                    os.environ["SNOW_REAL_VIEW_CONFIG"], os.environ["SNOW_REAL_VIEW_RUN_ID"] = config_file, run_id
                    flags = {"server_address": "127.0.0.1", "server_port": 8501, "server_headless": True,
                             "server_fileWatcherType": "none", "browser_gatherUsageStats": False}
                    bootstrap.load_config_options(flags)
                    bootstrap.run(str(root / "dashboard/real_app.py"), False, [], flags)
                    result = {"source": "real", "view_stopped": True}
                else:
                    spec = importlib.util.spec_from_file_location("snow_fixed_hive_cli", root / "tools/real_hive.py")
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    result = module.execute(config, operation, run_id, root=root)
            safe = {key: result[key] for key in ("confirmed", "verified", "source", "input_origin", "expires_at", "status", "view_stopped") if key in result}
            outgoing.write(frame_bytes(dict(schema_version=1, kind="complete", session=session, operation=operation, result=safe)))
            outgoing.flush()
    finally:
        finished.set()
        adapter.closed = True
        reader.closed.set()
        timer.cancel()


def node_guard(root, node):
    if os.name != "posix" or socket.gethostname() != node or str(root) != REMOTE_ROOT:
        raise ValueError("Hive internal phase reached the wrong fixed node or checkout")
