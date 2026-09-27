"""Read-only aggregate admission after actual finite engine storage retirement.

This never calls a live writer with an invented clock, never revives its job or
Checkpoint, and never treats a retired backend as an uninitialized backend.
"""
import re
from datetime import UTC, datetime, timedelta

from .io import digest
from .landing import collector_identity
from .lifecycle import timestamp
from .publication import canonical
from .real_epoch import LABEL_PREFIX, labels
from .real_quiescent import (
    WRITER_FILES,
    backend_resources,
    bind_ods,
    bounded_json,
    expected_parameters,
    same_keys,
    validate_initial,
)

BACKENDS = ("kafka", "doris", "checkpoint")
REGISTRATION_FIELDS = ("schema_version", "source", "input_origin", "epoch_id", "epoch_generation", "owner_manifest_sha256",
                       "collector", "original_min_accepted_at", "expires_at", "initialized_at", "storage", "writers",
                       "jar_sha256", "initial", "initial_readback_sha256")
RETIREMENT_FIELDS = ("source", "input_origin", "epoch_id", "generation", "owner_manifest_sha256", "original_min_accepted_at",
                     "expires_at", "containers_stopped", "containers_removed", "volumes_removed", "readback_absent", "checked_at",
                     "physical_storage_scope", "forensic_media_erasure_claimed", "production_online_database_touched", "evidence_sha256")


def sha(value):
    if not isinstance(value, str) or not re.fullmatch(r"[a-f0-9]{64}", value):
        raise ValueError("Invalid archived metadata hash")
    return value


def archived_writer(epoch, snapshot):
    """Validate immutable historical evidence without opening old JAR or code."""
    manifest = epoch.read()
    if manifest["mode"] != "engines" or manifest["input_origin"] != "real":
        raise ValueError("Synthetic retirement cannot authorize real aggregate reads")
    same_keys(manifest.get("files", {}), ("compose.json", "epoch-guard.sh", "fe.conf", "be.conf", "prepare-be-start.sh"), "archived configuration")
    for value in manifest["files"].values():
        sha(value)
    same_keys(manifest.get("jar", {}), ("path", "sha256"), "archived JAR")
    sha(manifest["jar"]["sha256"])
    value = bounded_json(epoch.directory / "writer-registration.json")
    same_keys(value, REGISTRATION_FIELDS, "archived writer")
    expected = dict(schema_version=1, source="real", input_origin="real", epoch_id=manifest["epoch_id"],
                    epoch_generation=manifest["generation"], owner_manifest_sha256=digest(canonical(manifest)),
                    original_min_accepted_at=manifest["original_min_accepted_at"], expires_at=manifest["expires_at"],
                    jar_sha256=manifest["jar"]["sha256"])
    if type(value["schema_version"]) is not int or any(value[key] != actual for key, actual in expected.items()):
        raise ValueError("Archived writer belongs to another generation or original window")
    if not timestamp(value["original_min_accepted_at"]) <= timestamp(value["initialized_at"]) < timestamp(value["expires_at"]):
        raise ValueError("Writer was not initialized within its original epoch")
    collector_identity(value["collector"])
    if not isinstance(value["writers"], dict) or set(value["writers"]) != set(WRITER_FILES):
        raise ValueError("Archived writer source inventory is incomplete")
    for token in value["writers"].values():
        sha(token)  # Historical hashes, deliberately not compared to today's source.
    validate_initial(value["initial"], manifest)
    if value["initial_readback_sha256"] != digest(canonical(value["initial"])):
        raise ValueError("Archived initialization readback changed")
    stored = value["storage"]
    same_keys(stored, ("containers", "volumes"), "archived storage")
    if set(stored["containers"]) != set(manifest["containers"].values()) or set(stored["volumes"]) != set(manifest["volumes"].values()):
        raise ValueError("Archived storage does not cover every owned engine object")
    for entry in stored["containers"].values():
        same_keys(entry, ("object_id", "created_at"), "archived container")
        sha(entry["object_id"])
        timestamp(entry["created_at"])
    for entry in stored["volumes"].values():
        same_keys(entry, ("created_at", "labels"), "archived volume")
        timestamp(entry["created_at"])
        if any(entry["labels"].get(key) != actual for key, actual in labels(manifest).items()):
            raise ValueError("Archived volume labels changed")
    job = bounded_json(epoch.directory / "writer-job.json")
    same_keys(job, ("schema_version", "source", "registration_sha256", "observed_at", "readback", "evidence_sha256"), "archived job")
    details = job["readback"]
    same_keys(details, ("job_id", "state", "jar_sha256", "parameters"), "archived job readback")
    if (type(job["schema_version"]) is not int or job["schema_version"] != 1 or job["source"] != "real" or
            job["registration_sha256"] != digest((epoch.directory / "writer-registration.json").read_bytes()) or
            job["evidence_sha256"] != digest(canonical(details)) or details["state"] != "RUNNING" or
            details["jar_sha256"] != value["jar_sha256"] or not re.fullmatch(r"[a-f0-9]{32}", details["job_id"]) or
            details["parameters"] != expected_parameters(manifest, value["initial"]["kafka"]) or
            not timestamp(value["initialized_at"]) <= timestamp(job["observed_at"]) < timestamp(value["expires_at"])):
        raise ValueError("Archived actual writer job is missing or changed")
    bind_ods(value, snapshot)
    return value


def verify_retirement(value, manifest, now):
    same_keys(value, RETIREMENT_FIELDS, "physical retirement receipt")
    expected = dict(source="real", input_origin="real", epoch_id=manifest["epoch_id"], generation=manifest["generation"],
                    owner_manifest_sha256=digest(canonical(manifest)), original_min_accepted_at=manifest["original_min_accepted_at"],
                    expires_at=manifest["expires_at"], physical_storage_scope="Docker objects and writable layers",
                    forensic_media_erasure_claimed=False, production_online_database_touched=False,
                    readback_absent={"containers": sorted(manifest["containers"].values()), "volumes": sorted(manifest["volumes"].values())})
    if (any(value[key] != actual for key, actual in expected.items()) or
            value["forensic_media_erasure_claimed"] is not False or value["production_online_database_touched"] is not False or
            not timestamp(manifest["expires_at"]) <= timestamp(value["checked_at"]) <= now or
            value["evidence_sha256"] != digest(canonical({key: item for key, item in value.items() if key != "evidence_sha256"}))):
        raise ValueError("Physical retirement evidence does not bind the original real epoch")
    for key, kind in (("containers_stopped", "containers"), ("containers_removed", "containers"), ("volumes_removed", "volumes")):
        entries = value[key]
        if not isinstance(entries, list) or len(set(entries)) != len(entries) or not set(entries) <= set(manifest[kind].values()):
            raise ValueError("Retirement receipt contains an unowned removed object")
    if value["containers_stopped"] != value["containers_removed"]:
        raise ValueError("Retirement did not remove every stopped container")
    return value


def inspect_absence(epoch, manifest, *, allow_expected=False):
    """Actual full inventory, including unknown aliases with same epoch labels."""
    containers, volumes = epoch.docker.containers(), epoch.docker.volumes()
    if len(containers) > 256 or len(volumes) > 512:
        raise ValueError("Docker inventory exceeds the bounded retirement audit")
    prefix = manifest["project"] + "-"
    wanted_c, wanted_v = set(manifest["containers"].values()), set(manifest["volumes"].values())
    if not allow_expected and (containers & wanted_c or volumes & wanted_v):
        raise ValueError("Retired storage object reappeared under its original name")

    def same_epoch(name, values):
        return (name.startswith(prefix) or values.get(LABEL_PREFIX + "epoch") == manifest["epoch_id"] or
                values.get(LABEL_PREFIX + "generation") == manifest["generation"] or
                values.get("com.docker.compose.project") == manifest["project"])

    for name in containers:
        if allow_expected and name in wanted_c:
            continue  # Epoch.retire performs full ownership validation itself.
        value = epoch.docker.inspect_container(name)
        if (same_epoch(name, value["labels"]) or
                any(m.get("Name") in wanted_v for m in value["mounts"] if m["Type"] == "volume")):
            raise ValueError("An unregistered container belongs to the retired epoch")
    for name in volumes:
        if allow_expected and name in wanted_v:
            continue
        value = epoch.docker.inspect_volume(name)
        if same_epoch(name, value.get("Labels") or {}):
            raise ValueError("An unregistered volume belongs to the retired epoch")
    # Detect inventory changes during the bounded read; retry as a new operation.
    if containers != epoch.docker.containers() or volumes != epoch.docker.volumes():
        raise ValueError("Docker inventory changed during the retirement absence check")
    return dict(containers=sorted(containers), volumes=sorted(volumes))


class RetiredStorage:
    def __init__(self, epoch, snapshot):
        self.epoch, self.snapshot = epoch, snapshot

    def registration(self):
        return archived_writer(self.epoch, self.snapshot)

    def check(self):
        now, manifest = datetime.now(UTC), self.epoch.read()
        if manifest["mode"] != "engines" or manifest["input_origin"] != "real" or timestamp(manifest["expires_at"]) > now:
            raise ValueError("Retired admission requires an actually expired real engine epoch")
        value = self.registration()
        existing = self.epoch.directory / "retirement.json"
        if existing.exists():
            verify_retirement(bounded_json(existing), manifest, now)
        inspect_absence(self.epoch, manifest, allow_expected=not existing.exists())
        # Not an imported JSON approval: this actual API removes the exact owned
        # expired Docker objects, or verifies absence for an existing receipt.
        retired = self.epoch.retire()
        verify_retirement(retired, manifest, datetime.now(UTC))
        inventory = inspect_absence(self.epoch, manifest)
        if self.registration() != value:
            raise ValueError("Historical registration changed during retirement verification")
        return dict(schema_version=1, source="real", input_origin="real", verification="retired_storage_absent",
                    epoch_id=manifest["epoch_id"], epoch_generation=manifest["generation"], collector=value["collector"],
                    owner_manifest_sha256=digest(canonical(manifest)), registration_sha256=digest((self.epoch.directory / "writer-registration.json").read_bytes()),
                    writer_job_sha256=digest((self.epoch.directory / "writer-job.json").read_bytes()),
                    original_min_accepted_at=manifest["original_min_accepted_at"], expires_at=manifest["expires_at"],
                    checked_at=datetime.now(UTC).isoformat(), retirement_sha256=digest(canonical(retired)),
                    inventory_sha256=digest(canonical(inventory)), physical_storage_absent=True,
                    sql_cleanup_verified=False, kafka_records_scanned=False, writer_restore_allowed=False,
                    raw_compute_allowed=False)

    def adapters(self):
        return {name: RetiredBackend(self, name) for name in BACKENDS}


class RetiredBackend:
    def __init__(self, retired, name):
        if name not in BACKENDS:
            raise ValueError("Unknown retired backend")
        self.retired, self.name = retired, name

    def verify_retired(self, resources, now):
        evidence = self.retired.check()
        if resources != backend_resources(self.retired.registration())[self.name]:
            raise ValueError("Retirement must cover the complete registered engine scope")
        return dict(backend=self.name, verification="retired_storage_absent", resources_sha256=digest(canonical(resources)),
                    checked_at=evidence["checked_at"], evidence=evidence, evidence_sha256=digest(canonical(evidence)), next_expiry=None)


def validate_retired_receipt(receipt, name, resources, owner, snapshot, now):
    """Only used after an in-process RetiredBackend ran the actual checks."""
    same_keys(receipt, ("backend", "verification", "resources_sha256", "checked_at", "evidence", "evidence_sha256", "next_expiry"), "retired receipt")
    value = receipt["evidence"]
    same_keys(value, ("schema_version", "source", "input_origin", "verification", "epoch_id", "epoch_generation", "collector",
                      "owner_manifest_sha256", "registration_sha256", "writer_job_sha256", "original_min_accepted_at", "expires_at",
                      "checked_at", "retirement_sha256", "inventory_sha256", "physical_storage_absent", "sql_cleanup_verified",
                      "kafka_records_scanned", "writer_restore_allowed", "raw_compute_allowed"), "retired evidence")
    if (receipt["backend"] != name or receipt["verification"] != "retired_storage_absent" or
            receipt["resources_sha256"] != digest(canonical(resources)) or receipt["evidence_sha256"] != digest(canonical(value)) or
            value["verification"] != receipt["verification"] or value["source"] != "real" or value["input_origin"] != "real" or
            type(value["schema_version"]) is not int or value["schema_version"] != 1 or
            value["collector"] != snapshot["identity"]["collector"] or any(value["collector"][key] != owner[key] for key in ("instance_id", "generation")) or
            value["physical_storage_absent"] is not True or
            any(value[key] is not False for key in ("sql_cleanup_verified", "kafka_records_scanned", "writer_restore_allowed", "raw_compute_allowed")) or
            receipt["checked_at"] != value["checked_at"] or not now <= timestamp(value["checked_at"]) <= now + timedelta(minutes=10) or
            receipt["next_expiry"] is not None or timestamp(value["expires_at"]) > now or
            timestamp(value["expires_at"]) != timestamp(value["original_min_accepted_at"]) + timedelta(days=7)):
        raise ValueError("Invalid retired-storage absence evidence")
    for key in ("owner_manifest_sha256", "registration_sha256", "writer_job_sha256", "retirement_sha256", "inventory_sha256"):
        sha(value[key])
    return receipt
