"""Registered remote real copies, verified cleanup and short-lived read permits.

Backend callbacks execute actual scoped checks; this module never accepts a JSON
approval flag in place of a backend check. An uninitialized backend is recorded
as outside this permit's scope, never described as a verified running backend.
"""
import json
import re
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from urllib.parse import urlsplit
from uuid import UUID

from .io import digest, write_json
from .landing import checked_receipt, load
from .lifecycle import RETENTION_DAYS, timestamp
from .publication import canonical, publication_lock
from .real_behavior import HK, real_path, validate_auxiliary_manifest, validate_coverage
from .real_ods import expire_window

BACKENDS = ("kafka", "doris", "checkpoint", "hive")


def outputs_for(job):
    token = job["run_id"]
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", token) or job["source"] != "real":
        raise ValueError("Invalid real job identity")
    return dict(daily=job["warehouse_root"].rstrip("/") + "/runs/" + token,
                behavior=job["warehouse_root"].rstrip("/") + "/model-runs/" + token + "/behavior",
                auxiliary=job["auxiliary_root"].rstrip("/") + "/" + token)


def hive_tables_for(job):
    if type(job.get("register_hive", False)) is not bool:
        raise ValueError("Hive registration must be an explicit boolean")
    if not job.get("register_hive", False):
        return []
    token = job["run_id"].replace("-", "_")
    return ["snow_real.ads_" + token] + ["snow_real.behavior_" + token + "_" + name
                                        for name in ("sessions", "session_daily", "retention", "conversions", "funnel")]


class WebHdfsOwned:
    """Adapter around the existing pinned-host HdfsSink HTTP client."""
    def __init__(self, sink):
        self.sink = sink

    def path(self, uri):
        parsed = urlsplit(uri)
        if parsed.scheme != "hdfs" or parsed.hostname != self.sink.host or parsed.port != 9000:
            raise ValueError("HDFS lifecycle request escaped its configured NameNode")
        return parsed.path

    def exists(self, uri):
        try:
            self.sink.request("GET", self.path(uri), "GETFILESTATUS")
            return True
        except FileNotFoundError:
            return False

    def children(self, uri):
        try:
            entries = self.sink.request("GET", self.path(uri), "LISTSTATUS").json()["FileStatuses"]["FileStatus"]
        except FileNotFoundError:
            return []
        if len(entries) > 10000:
            raise ValueError("Bounded HDFS inventory limit exceeded")
        result = []
        for entry in entries:
            name = entry["pathSuffix"]
            if not name or "/" in name or name in {".", ".."} or entry["type"] not in {"FILE", "DIRECTORY"}:
                raise ValueError("Unexpected path or linked object in real inventory")
            result.append((uri.rstrip("/") + "/" + name, entry["type"]))
        return result

    def delete_exact(self, uri):
        try:
            response = self.sink.request("DELETE", self.path(uri), "DELETE", recursive="true")
            if not response.json()["boolean"]:
                raise RuntimeError("HDFS exact resource deletion failed")
        except FileNotFoundError:
            pass
        if self.exists(uri):
            raise RuntimeError("Expired HDFS resource remains after deletion")


class RealRemoteLifecycle:
    def __init__(self, directory):
        self.directory = Path(directory).absolute()
        self.registry = self.directory / "registry.json"
        self.owner = self.directory / "owner.json"
        self.journal = self.directory / "cleanup.json"

    def initialize(self, warehouse_root, auxiliary_root, ods_root, instance_id, generation):
        real_path(warehouse_root, "warehouse")
        real_path(auxiliary_root, "auxiliary")
        if not re.fullmatch(r"hdfs://[A-Za-z0-9.-]+:9000/snow/ods/real/kafka/[a-z0-9-]{1,60}", ods_root):
            raise ValueError("Only an exact real ODS lane is allowed")
        if len({urlsplit(root).netloc for root in (warehouse_root, auxiliary_root, ods_root)}) != 1:
            raise ValueError("All registered HDFS roots must share this NameNode")
        UUID(instance_id)
        UUID(generation)
        if self.directory.exists() and any(self.directory.iterdir()):
            raise ValueError("Remote lifecycle registry needs a new empty private metadata directory")
        self.directory.mkdir(parents=True, exist_ok=True)
        if self.directory.resolve() != self.directory:
            raise ValueError("Registry cannot traverse directory links")
        write_json(self.owner, dict(project="snow-statistics", source="real", instance_id=instance_id, generation=generation,
                                   roots=dict(warehouse=warehouse_root, auxiliary=auxiliary_root, ods=ods_root)))
        write_json(self.registry, dict(schema_version=1, artifacts={}, backends={name: {"state": "not_initialized", "resources": {}} for name in BACKENDS}))

    def _read(self):
        if self.directory.resolve() != self.directory:
            raise ValueError("Remote metadata owner path changed")
        owner, registry = load(self.owner), load(self.registry)
        if owner["project"] != "snow-statistics" or owner["source"] != "real" or registry["schema_version"] != 1:
            raise ValueError("Invalid real remote ownership")
        if set(registry["backends"]) != set(BACKENDS):
            raise ValueError("Unexpected backend registry")
        for entry in registry["artifacts"].values():
            if (entry["kind"] not in RETENTION_DAYS or
                    timestamp(entry["expires_at"]) > timestamp(entry["original_min_accepted_at"]) + timedelta(days=RETENTION_DAYS[entry["kind"]]) or
                    any(ref not in registry["artifacts"] for ref in entry["references"])):
                raise ValueError("Invalid retained lifetime or physical reference")
        for scope in registry["backends"].values():
            if scope["state"] not in {"not_initialized", "initialized"} or scope["state"] == "not_initialized" and scope["resources"]:
                raise ValueError("Initialized backend cannot masquerade as an empty scope")
            for entry in scope["resources"].values():
                if entry["kind"] not in RETENTION_DAYS or timestamp(entry["expires_at"]) > timestamp(entry["original_min_accepted_at"]) + timedelta(days=RETENTION_DAYS[entry["kind"]]):
                    raise ValueError("Backend registry extends original retention")
        return owner, registry

    def owned(self, uri):
        owner, _ = self._read()
        for kind in ("warehouse", "auxiliary"):
            real_path(uri, kind) if uri.startswith(owner["roots"][kind] + "/") else None
            if uri.startswith(owner["roots"][kind] + "/"):
                return uri
        raise ValueError("Resource is not a descendant of a registered real root")

    def register(self, uri, kind, original_at, *, copied_from=None, references=(), now=None):
        self.owned(uri)
        if kind not in RETENTION_DAYS:
            raise ValueError("Unknown retention category")
        current, origin = now or datetime.now(UTC), timestamp(original_at)
        expires = origin + timedelta(days=RETENTION_DAYS[kind])
        if origin > current or expires <= current:
            raise ValueError("Cannot reserve an expired or future original record")
        with publication_lock(self.directory):
            _, data = self._read()
            items = data["artifacts"]
            entry = dict(kind=kind, original_min_accepted_at=origin.isoformat(), expires_at=expires.isoformat(),
                         copied_from=copied_from, references=list(references))
            if uri in items:
                if items[uri] != entry:
                    raise ValueError("Copy or retry cannot extend an immutable resource lifetime")
                return uri
            if self.journal.exists():
                raise ValueError("Resolve failed cleanup before registering additional payload")
            if any(uri.startswith(key + "/") or key.startswith(uri + "/") for key in items):
                raise ValueError("Overlapping remote retention roots")
            if any(ref not in items for ref in references):
                raise ValueError("Unregistered physical resource reference")
            if copied_from is not None:
                old = items[copied_from]
                if old["kind"] != kind or timestamp(old["original_min_accepted_at"]) != origin:
                    raise ValueError("Copy cannot reset its original acceptance timestamp")
            items[uri] = entry
            write_json(self.registry, data)
        return uri

    def register_backend(self, backend, resource, kind, original_at, *, now=None):
        if backend not in BACKENDS or kind not in RETENTION_DAYS:
            raise ValueError("Unknown backend or retention category")
        patterns = dict(kafka=r"snow\.real\.[a-zA-Z0-9_.-]{1,150}", doris=r"snow_real_[a-z0-9_]+\.[a-z0-9_]+",
                        hive=r"snow_real\.[a-z0-9_]+", checkpoint=r"(?:hdfs://[a-zA-Z0-9.-]+:9000/snow/checkpoints/real/|/opt/snow/runtime/real/checkpoints/)[A-Za-z0-9_/-]{1,150}")
        if not re.fullmatch(patterns[backend], resource) or ".." in resource:
            raise ValueError("Backend resource outside real namespace")
        origin, current = timestamp(original_at), now or datetime.now(UTC)
        if origin > current or origin + timedelta(days=RETENTION_DAYS[kind]) <= current:
            raise ValueError("Cannot register already expired backend payload")
        entry = dict(kind=kind, original_min_accepted_at=origin.isoformat(), expires_at=(origin + timedelta(days=RETENTION_DAYS[kind])).isoformat())
        with publication_lock(self.directory):
            _, data = self._read()
            scope = data["backends"][backend]
            if resource in scope["resources"] and scope["resources"][resource] != entry:
                raise ValueError("A backend copy cannot renew expiry")
            if self.journal.exists() and resource not in scope["resources"]:
                raise ValueError("Resolve failed cleanup before registering another backend resource")
            scope["resources"][resource], scope["state"] = entry, "initialized"
            write_json(self.registry, data)

    def reserve_job(self, job, original_raw_at, original_auxiliary_at, *, now=None):
        outputs = outputs_for(job)
        aggregate_at = datetime.combine(datetime.fromisoformat(job["date_from"]).date(), time(), HK).isoformat()
        for name in ("dwd", "quarantine"):
            self.register(outputs["daily"] + "/" + name, "raw", original_raw_at, now=now)
        for name in ("ads_daily", "accepted", "publication"):
            self.register(outputs["daily"] + "/" + name, "aggregate", aggregate_at, now=now)
        for name in ("sessions", "conversions"):
            self.register(outputs["behavior"] + "/" + name, "auxiliary", original_auxiliary_at, now=now)
        for name in ("session_daily", "retention", "funnel", "accepted"):
            self.register(outputs["behavior"] + "/" + name, "aggregate", aggregate_at, now=now)
        self.register(outputs["auxiliary"], "auxiliary", original_auxiliary_at, now=now)
        for table in hive_tables_for(job):
            detailed = table.endswith(("_sessions", "_conversions"))
            self.register_backend("hive", table, "auxiliary" if detailed else "aggregate",
                                  original_auxiliary_at if detailed else aggregate_at, now=now)
        return outputs

    def inventory(self, backend, artifacts):
        owner, _ = self._read()
        pending = [owner["roots"][name] for name in ("warehouse", "auxiliary")]
        visited = 0
        while pending:
            parent = pending.pop()
            for uri, kind in backend.children(parent):
                visited += 1
                if visited > 10000:
                    raise ValueError("Remote inventory exceeds bounded metadata scope")
                if uri in artifacts:
                    continue
                if kind == "DIRECTORY":
                    pending.append(uri)
                else:
                    raise ValueError("Unregistered real payload blocks all readers")

    def cleanup(self, hdfs, ods_directory, ods_sink, *, backend_checks=None, auxiliary_rewrites=None, now=None):
        """Return a receipt only after all initialized backend scopes are checked.

        A check is an in-process adapter with ``purge_and_verify(resources, now)``;
        its receipt must bind the exact registry digest and report zero expired
        records. Missing adapters fail closed. Empty, never-initialized scopes are
        explicitly not certified by this receipt.
        """
        current = now or datetime.now(UTC)
        backend_checks, auxiliary_rewrites = backend_checks or {}, auxiliary_rewrites or {}
        with publication_lock(self.directory):
            owner, data = self._read()
            checkpoint = load(self.journal)
            registry_hash = digest(canonical(data))
            if checkpoint is None:
                checkpoint = dict(source="real", started_at=current.isoformat(), status="cleanup_in_progress",
                                  registry_sha256=registry_hash, completed={})
                write_json(self.journal, checkpoint)
            elif checkpoint.get("source") != "real" or checkpoint.get("registry_sha256") != registry_hash:
                raise ValueError("Resolve prior failed cleanup before changing its resource registry")
            if owner["roots"]["ods"] != ods_sink.root:
                raise ValueError("ODS cleanup destination differs from owned lane")
            ods_result = expire_window(ods_directory, ods_sink, now=current)
            state = load(Path(ods_directory) / "state.json")
            if state is None:
                raise ValueError("Real read permit requires a committed ODS head")
            snapshot = checked_receipt(state)
            if snapshot["source"] != "real" or snapshot["schema_version"] != 2:
                raise ValueError("Unexpected ODS source")
            body = ods_sink.read(urlsplit(state["input"]).path)
            if digest(body) != state["snapshot_id"]:
                raise ValueError("ODS snapshot readback differs from its committed hash")
            if any(timestamp(entry["expires_at"]) <= current for entry in snapshot["batches"]):
                raise ValueError("Expired ODS references remain")
            if any(timestamp(entry["expires_at"]) != timestamp(entry["original_min_accepted_at"]) + timedelta(days=7) for entry in snapshot["batches"]):
                raise ValueError("ODS copy reset its original raw retention deadline")
            self.inventory(hdfs, data["artifacts"])
            expired = {key for key, entry in data["artifacts"].items() if timestamp(entry["expires_at"]) <= current}
            while True:
                dependent = {key for key, entry in data["artifacts"].items() if set(entry["references"]) & expired}
                if dependent <= expired:
                    break
                expired |= dependent
            removed, rewritten = [], []
            for uri in sorted(expired):
                self.owned(uri)
                entry = data["artifacts"][uri]
                completed = checkpoint["completed"].get(uri)
                if completed:
                    if hdfs.exists(uri):
                        raise ValueError("A previously deleted real resource reappeared")
                    if completed["action"] == "rewritten":
                        replacement = completed["replacement"]
                        if not hdfs.exists(replacement["path"]):
                            raise ValueError("Registered auxiliary replacement is missing")
                        rewritten.append(replacement)
                elif uri in auxiliary_rewrites:
                    # Caller adapter performs inspect -> pre-reserve fresh target
                    # in its own journal -> prune_auxiliary_hdfs -> exact readback.
                    replacement = auxiliary_rewrites[uri].prune_and_verify(uri, entry, current)
                    target = self.owned(replacement["path"])
                    if target not in data["artifacts"] or hdfs.exists(uri) or not hdfs.exists(target):
                        raise ValueError("Auxiliary rewrite was not pre-registered and verified")
                    if timestamp(replacement["expires_at"]) <= current:
                        raise ValueError("Auxiliary rewrite retained expired tokens")
                    reserved = data["artifacts"][target]
                    if (reserved["kind"] != "auxiliary" or timestamp(reserved["original_min_accepted_at"]) != timestamp(replacement["original_min_accepted_at"]) or
                            timestamp(reserved["expires_at"]) != timestamp(replacement["expires_at"])):
                        raise ValueError("Auxiliary rewrite changed the reserved original lifetime")
                    rewritten.append(replacement)
                    checkpoint["completed"][uri] = {"action": "rewritten", "replacement": replacement}
                else:
                    hdfs.delete_exact(uri)
                    if hdfs.exists(uri):
                        raise RuntimeError("Expired registered HDFS resource remains")
                    checkpoint["completed"][uri] = {"action": "deleted"}
                removed.append(uri)
                write_json(self.journal, checkpoint)
            receipts = {}
            for name, scope in data["backends"].items():
                if scope["state"] == "not_initialized" and not scope["resources"]:
                    receipts[name] = {"scope": "not_initialized", "certified": False}
                    continue
                adapter = backend_checks.get(name)
                if adapter is None:
                    raise ValueError("Missing actual initialized backend check: " + name)
                receipt = adapter.purge_and_verify(scope["resources"], current)
                if (receipt.get("backend") != name or receipt.get("resources_sha256") != digest(canonical(scope["resources"])) or
                        type(receipt.get("remaining_expired")) is not int or receipt["remaining_expired"] != 0 or
                        not current <= timestamp(receipt["checked_at"]) <= current + timedelta(minutes=10) or
                        not re.fullmatch(r"[a-f0-9]{64}", receipt.get("evidence_sha256", ""))):
                    raise ValueError("Backend did not verify the exact registered scope: " + name)
                live = receipt.get("live_records")
                next_expiry = receipt.get("next_expiry")
                if (type(live) is not int or live < 0 or "next_expiry" not in receipt or
                        live == 0 and next_expiry is not None or
                        live > 0 and (next_expiry is None or timestamp(next_expiry) <= current)):
                    raise ValueError("Backend must report the next original expiry or prove its scope empty")
                receipts[name] = receipt
            for uri in removed:
                del data["artifacts"][uri]
            write_json(self.registry, data)
            expiries = [entry["expires_at"] for entry in data["artifacts"].values()]
            expiries.extend(value["next_expiry"] for value in receipts.values() if value.get("next_expiry"))
            result = dict(schema_version=1, source="real", owner=dict(instance_id=owner["instance_id"], generation=owner["generation"]),
                          checked_at=current.isoformat(), registry_sha256=digest(canonical(data)),
                          input_snapshot=state["input"], ods=ods_result, removed=removed, auxiliary_rewrites=rewritten,
                          backends=receipts, next_expiry=min(expiries, key=timestamp) if expiries else None)
            write_json(self.directory / "last-cleanup.json", result)
            self.journal.unlink()
            return result

    def issue_permit(self, job, coverage_file, auxiliary_file, permit_file, hdfs, ods_directory, ods_sink, *,
                     backend_checks=None, auxiliary_rewrites=None, now=None):
        current = now or datetime.now(UTC)
        target = Path(permit_file)
        if target.exists():
            old_permit = load(target)
            if old_permit.get("source") != "real" or old_permit.get("schema_version") != 1:
                raise ValueError("Refuse to overwrite a file that is not an owned real permit")
            write_json(target, dict(schema_version=1, source="real", status="blocked_pending_cleanup", expires_at=current.isoformat()))
        coverage_bytes = Path(coverage_file).read_bytes()
        owner, data = self._read()
        coverage = validate_coverage(json.loads(coverage_bytes), job["cutoff"], owner)
        outputs = outputs_for(job)
        expected = ([outputs["daily"] + "/" + name for name in ("dwd", "quarantine", "ads_daily", "accepted", "publication")] +
                    [outputs["behavior"] + "/" + name for name in ("sessions", "conversions", "session_daily", "retention", "funnel", "accepted")] + [outputs["auxiliary"]])
        if any(path not in data["artifacts"] for path in expected):
            raise ValueError("Reserve every prospective output before issuing a read permit")
        receipt = self.cleanup(hdfs, ods_directory, ods_sink, backend_checks=backend_checks, auxiliary_rewrites=auxiliary_rewrites, now=current)
        if receipt["input_snapshot"] != job["input"]:
            raise ValueError("ODS cleanup changed the window; bind the new snapshot explicitly")
        auxiliary_bytes = Path(auxiliary_file).read_bytes() if auxiliary_file else None
        if auxiliary_bytes:
            descriptor = validate_auxiliary_manifest(json.loads(auxiliary_bytes), coverage, current)
            _, after = self._read()
            if descriptor["path"] not in after["artifacts"] or not hdfs.exists(descriptor["path"]):
                raise ValueError("Auxiliary read target is not a registered live resource")
        _, after = self._read()
        if self.journal.exists() or any(path not in after["artifacts"] for path in expected):
            raise ValueError("Cleanup did not leave every output reservation readable")
        expires = min(current + timedelta(minutes=15), timestamp(receipt["next_expiry"]))
        if expires <= current:
            raise ValueError("A real resource expires before a read permit can be used")
        receipt_file = target.with_suffix(".lifecycle.json")
        write_json(receipt_file, receipt)
        permit = dict(schema_version=1, source="real", input_snapshot=job["input"], coverage_sha256=digest(coverage_bytes),
                      auxiliary_sha256=digest(auxiliary_bytes) if auxiliary_bytes else None,
                      outputs=outputs, hive_tables=hive_tables_for(job), issued_at=current.isoformat(), expires_at=expires.isoformat(),
                      lifecycle_receipt_sha256=digest(receipt_file.read_bytes()))
        write_json(target, permit)
        return permit
