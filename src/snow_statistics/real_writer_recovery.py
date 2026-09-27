"""Controlled Flink 1.20.3 retained-checkpoint pause/resume, metadata only.

This uses actual Docker/Flink readbacks. Synthetic test adapters are not engine
acceptance. There is no argument for a JobID, checkpoint path or success JSON.
"""
import os
import re
import time
from datetime import UTC, datetime

from .io import digest
from .publication import canonical
from .real_quiescent import (
    RecoveryLedger,
    checkpoint_files,
    checkpoint_metadata,
    expected_parameters,
    storage,
)
from .real_writer import job_environment

TERMINAL = {"CANCELED", "FAILED", "FINISHED", "SUSPENDED"}
JAR = "/opt/flink/usrlib/snow-realtime.jar"


def normalize_checkpoint(path, job_id, checkpoint_id):
    expected = f"/checkpoints/{job_id}/chk-{checkpoint_id}"
    if path not in {"file:" + expected, "file://" + expected}:
        raise ValueError("Flink checkpoint is outside the exact registered job volume")
    return "file://" + expected


class ActualRecoveryProbe:
    """No endpoint/command supplied by a config; all targets derive from epoch."""
    def __init__(self, writer):
        self.writer = writer
        self.epoch = writer.epoch
        self.manifest = writer.manifest
        self.submitted = None

    def jobs(self):
        value = self.writer.flink("/jobs/overview")["jobs"]
        if not isinstance(value, list) or len(value) > 1000:
            raise ValueError("Unexpected bounded Flink job inventory")
        return value

    def job(self, job_id):
        value = self.writer.flink("/jobs/" + job_id)
        if value["jid"] != job_id or value["name"] != "Snow Statistics real " + self.manifest["event_lane"]:
            raise ValueError("Actual Flink returned a different registered job")
        return value

    def verify_jar(self, registration):
        for role in ("jobmanager", "taskmanager"):
            raw = self.epoch.docker.command(["exec", self.manifest["containers"][role], "sha256sum", JAR]).decode().split()
            if len(raw) != 2 or raw != [registration["jar_sha256"], JAR]:
                raise ValueError("Actual mounted JAR differs from immutable writer registration")

    def latest(self, job_id, registration, now):
        config = self.writer.flink("/jobs/" + job_id + "/checkpoints/config")
        if config.get("externalization") != {"enabled": True, "delete_on_cancellation": False}:
            raise ValueError("Actual checkpoint configuration must retain state on cancellation")
        value = self.writer.flink("/jobs/" + job_id + "/checkpoints").get("latest", {}).get("completed")
        if (not isinstance(value, dict) or value.get("status") != "COMPLETED" or value.get("discarded") is not False
                or value.get("is_savepoint") is not False or value.get("checkpoint_type") not in {"CHECKPOINT", "UNALIGNED_CHECKPOINT"}
                or type(value.get("num_subtasks")) is not int or value["num_subtasks"] <= 0
                or value.get("num_acknowledged_subtasks") != value["num_subtasks"]):
            raise ValueError("No fully completed retained external checkpoint is available")
        result = dict(job_id=job_id, id=value["id"],
                      external_path=normalize_checkpoint(value["external_path"], job_id, value["id"]),
                      trigger_timestamp=value["trigger_timestamp"], latest_ack_timestamp=value["latest_ack_timestamp"])
        return checkpoint_metadata(result, job_id, registration, now)

    def cancel(self, job_id):
        import httpx
        with httpx.Client(base_url="http://127.0.0.1:8081", timeout=15, trust_env=False, follow_redirects=False) as client:
            result = client.patch("/jobs/" + job_id, params={"mode": "cancel"})
            if result.status_code != 202:
                raise ValueError("Owned Flink did not acknowledge cancellation")

    def tree(self, jobs, checkpoint):
        """Read hashes in place; do not copy checkpoint bytes to host or receipts.

        Full volume scope includes each previously registered job's shared state.
        No symlinks, hard links, foreign roots, savepoints or arbitrary paths.
        """
        jm = self.manifest["containers"]["jobmanager"]
        command = ["exec", jm, "find", "/checkpoints", "-xdev", "-mindepth", "1", "-printf", r"%y\0%n\0%s\0%P\0"]
        raw = self.epoch.docker.command(command, timeout=15)
        if len(raw) > 262144 or not raw.endswith(b"\0"):
            raise ValueError("Checkpoint inventory is missing or exceeds metadata capacity")
        parts = raw[:-1].decode().split("\0")
        if len(parts) % 4 or len(parts) > 8192:
            raise ValueError("Invalid bounded checkpoint inventory")
        entries, total = [], 0
        for i in range(0, len(parts), 4):
            kind, links, size, relative = parts[i:i + 4]
            if (not re.fullmatch(r"[a-f0-9]{32}(?:/[A-Za-z0-9_.-]+)*", relative)
                    or any(p in {".", ".."} for p in relative.split("/")) or relative.split("/")[0] not in jobs
                    or kind not in {"f", "d"} or not size.isdecimal() or kind == "f" and links != "1"):
                raise ValueError("Checkpoint volume contains an unregistered or linked resource")
            if kind == "f":
                total += int(size)
                entries.append((relative, int(size)))
        if not 1 <= len(entries) <= 512 or total > 128 * 1024 * 1024:
            raise ValueError("Checkpoint recovery exceeds its bounded file/byte capacity")
        result = []
        for relative, size in sorted(entries):
            path = "/checkpoints/" + relative
            output = self.epoch.docker.command(["exec", jm, "sha256sum", path], timeout=15).decode().split()
            if len(output) != 2 or output[1] != path:
                raise ValueError("Checkpoint hashing returned a different file")
            result.append(dict(path=relative, bytes=size, sha256=output[0]))
        if self.epoch.docker.command(command, timeout=15) != raw:
            raise ValueError("Checkpoint inventory changed during stopped-job hashing")
        return checkpoint_files(result, jobs, checkpoint)

    def submit(self, checkpoint, registration, sequence):
        environment = job_environment(self.manifest, registration, self.writer.account(), self.writer.host)
        path = self.writer.directory / f"recovery-{sequence:06d}.env"
        if path.exists() or path.resolve() != path.absolute():
            raise ValueError("A recovery environment cannot be reused or linked")
        # O_EXCL keeps a failed invocation auditable. Never overwrite the original
        # submission env or the four frozen epoch environment variables.
        with path.open("x", encoding="utf-8", newline="\n") as stream:
            os.chmod(path, 0o600)
            stream.write("".join(key + "=" + value + "\n" for key, value in environment.items()))
            stream.flush()
            os.fsync(stream.fileno())
        self.verify_jar(registration)
        output = self.epoch.docker.command(["exec", "--env-file", str(path), self.manifest["containers"]["jobmanager"],
                "/opt/flink/bin/flink", "run", "-d", "--fromSavepoint", checkpoint["external_path"],
                "--claimMode", "no_claim", "-c", "dev.xiaob.snow.RealtimeJob", JAR], timeout=120)
        matches = re.findall(rb"JobID\s+([a-f0-9]{32})", output)
        if len(matches) != 1:
            raise ValueError("Actual restore did not acknowledge exactly one JobID")
        job_id = matches[0].decode()
        self.submitted = dict(job_id=job_id, jar_sha256=registration["jar_sha256"],
                              parameters=expected_parameters(self.manifest, registration["initial"]["kafka"]))
        return dict(job_id=job_id, environment_sha256=digest(path.read_bytes()))

    def restored(self, job_id, checkpoint):
        if self.submitted is None or self.submitted["job_id"] != job_id:
            raise ValueError("No actual recovery submission is bound to this invocation")
        actual = self.job(job_id)
        value = self.writer.flink("/jobs/" + job_id + "/checkpoints")
        restored = value.get("latest", {}).get("restored")
        if actual["state"] != "RUNNING" or not isinstance(restored, dict) or value.get("counts", {}).get("restored", 0) < 1:
            return None
        # Flink's savepoint loader reports is_savepoint=true for checkpoints
        # passed via -s as well (verified in prior synthetic engine evidence).
        # Prove identity with the actual source checkpoint path/id, not that flag.
        if type(restored.get("is_savepoint")) is not bool:
            raise ValueError("Actual restored state metadata is incomplete")
        path = normalize_checkpoint(restored["external_path"], checkpoint["job_id"], restored["id"])
        if restored["id"] != checkpoint["id"] or path != checkpoint["external_path"]:
            raise ValueError("Actual restored checkpoint differs from registered exact checkpoint")
        return dict(job_id=job_id, readback=dict(state="RUNNING", **self.submitted),
                    restored=dict(id=restored["id"], external_path=path, restore_timestamp=restored["restore_timestamp"],
                                  is_savepoint=restored["is_savepoint"]))


class WriterRecovery:
    def __init__(self, writer, *, probe=None, clock=None, sleep=time.sleep):
        self.writer, self.registry = writer, writer.registry
        self.probe = probe or ActualRecoveryProbe(writer)
        self.clock = clock or (lambda: datetime.now(UTC))
        self.sleep = sleep
        self.ledger = RecoveryLedger(self.registry)

    def guard(self):
        value = self.registry.ready(self.writer.identity(), now=self.clock())
        if storage(self.writer.epoch, running=True) != value["storage"]:
            raise ValueError("Recovery requires the unchanged, running epoch storage")
        self.probe.verify_jar(value)
        return value

    def inventory(self, jobs, active):
        observed = self.probe.jobs()
        if (any(item.get("jid") not in jobs for item in observed)
                or len({item["jid"] for item in observed}) != len(observed)
                or {item["jid"] for item in observed if item.get("state") not in TERMINAL} != set(active)):
            raise ValueError("Actual Flink inventory contains an unregistered or unexpected active JobID")

    def known_jobs(self, history):
        from .real_quiescent import bounded_json
        return {bounded_json(self.registry.job_path)["readback"]["job_id"]} | {
            item["payload"]["job_id"] for item in history if item["action"] in {"resumed", "resume_acknowledged"}}

    def pause(self):
        # Lock acquisition is outside failure-stop: if another sync owns it,
        # fail immediately without disrupting that in-flight producer batch.
        with self.registry.operation_lock():
            try:
                registration = self.guard()
                history = self.ledger.read(now=self.clock())
                state = history[-1]["action"] if history else "resumed"
                if state not in {"resumed", "pause_requested"}:
                    raise ValueError("Writer is already paused or has an interrupted recovery; do not create another job")
                job_id = self.registry.current_job(now=self.clock())
                jobs = self.known_jobs(history)
                if state == "resumed":
                    self.ledger.append("pause_requested", dict(job_id=job_id), now=self.clock())
                state = self.probe.job(job_id)["state"]
                self.inventory(jobs, [job_id] if state == "RUNNING" else [])
                candidate = self.probe.latest(job_id, registration, self.clock())
                if state == "RUNNING":
                    self.probe.cancel(job_id)
                elif state != "CANCELED":
                    raise ValueError("Only a registered RUNNING/CANCELED job can finish controlled pause")
                for _ in range(120):
                    self.registry.ready(now=self.clock())
                    if self.probe.job(job_id)["state"] == "CANCELED":
                        break
                    self.sleep(0.5)
                else:
                    raise TimeoutError("Actual job cancellation did not finish within 60 seconds")
                self.inventory(jobs, [])
                checkpoint = self.probe.latest(job_id, registration, self.clock())
                if checkpoint["id"] < candidate["id"]:
                    raise ValueError("Latest checkpoint regressed during controlled cancellation")
                files = self.probe.tree(jobs, checkpoint)
                self.guard()
                self.inventory(jobs, [])
                result = self.ledger.append("paused", dict(job_id=job_id, checkpoint=checkpoint, files=files,
                    files_sha256=digest(canonical(files)), storage_sha256=digest(canonical(registration["storage"])), state="CANCELED"), now=self.clock())
                self.writer.epoch.stop()
                if storage(self.writer.epoch, running=False) != registration["storage"]:
                    raise ValueError("Actual stopped epoch changed during pause")
                return result
            except BaseException:
                self.writer.epoch.stop()
                raise

    def resume(self):
        with self.registry.operation_lock():
            try:
                registration = self.guard()
                history = self.ledger.read(now=self.clock())
                if not history or history[-1]["action"] != "paused":
                    raise ValueError("Resume needs a completed controlled pause; interrupted submissions require inspection")
                paused = history[-1]["payload"]
                checkpoint = paused["checkpoint"]
                jobs = self.known_jobs(history)
                self.inventory(jobs, [])
                if self.probe.tree(jobs, checkpoint) != paused["files"]:
                    raise ValueError("Retained checkpoint files are missing or changed")
                self.guard()
                intent = self.ledger.append("resume_requested", dict(job_id=paused["job_id"],
                    checkpoint_sha256=digest(canonical(checkpoint)), restore_mode="NO_CLAIM"), now=self.clock())
                acknowledged = self.probe.submit(checkpoint, registration, intent["sequence"])
                self.ledger.append("resume_acknowledged", dict(from_job_id=paused["job_id"],
                    checkpoint_sha256=digest(canonical(checkpoint)), **acknowledged), now=self.clock())
                job_id = acknowledged["job_id"]
                for _ in range(240):
                    self.registry.ready(now=self.clock())
                    details = self.probe.job(job_id)
                    if details["state"] in TERMINAL:
                        raise ValueError("Recovered Flink job terminated before readiness")
                    restored = self.probe.restored(job_id, checkpoint)
                    if restored is not None:
                        self.guard()
                        self.inventory(jobs | {job_id}, [job_id])
                        return self.ledger.append("resumed", restored, now=self.clock())
                    self.sleep(0.5)
                raise TimeoutError("Recovered job did not read back RUNNING with the exact checkpoint within 120 seconds")
            except BaseException:
                self.writer.epoch.stop()
                raise
