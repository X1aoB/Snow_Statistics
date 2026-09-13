"""Bounded aggregate-only copies between control, operator and analysis nodes."""
import json
import re
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

from .io import atomic_write, digest, write_json
from .landing import collector_identity
from .lifecycle import RealLifecycle, timestamp
from .publication import publication_lock
from .real_behavior import HK
from .real_publication import read_real_release, real_release, release_real

MAX_BYTES = 4 * 1024**2
FIELDS = {"schema_version", "source", "kind", "lane", "run_id", "file", "sha256", "bytes",
          "date_from", "date_to", "original_at", "expires_at", "collector", "input", "snapshot_id"}


def paths(lane, run_id):
    if not re.fullmatch(r"[a-z][a-z0-9-]{2,23}", lane) or not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", run_id or ""):
        raise ValueError("An exact owned release lane/run ID is required")
    root = f"runtime/real/transfers/{lane}/{run_id}"
    return dict(directory=root, incoming_manifest=root + "/incoming-manifest.json", manifest=root + "/manifest.json",
                pair=root + "/data/pair.json", incoming_pair=root + "/data/pair.json.tmp", published=root + "/published")


def read(path, limit=MAX_BYTES):
    path = Path(path).absolute()
    if path.resolve() != path or not path.is_file() or path.stat().st_size > limit:
        raise ValueError("Transferred artifact must be a bounded regular file without links")
    return path.read_bytes()


def metadata(release, payload, lane, run_id):
    manifest = release["daily"]["manifest"]
    snapshot = manifest["input_snapshot"]
    return dict(schema_version=1, source="real", kind="aggregate_pair", lane=lane, run_id=run_id, file="pair.json",
                sha256=digest(payload), bytes=len(payload), date_from=manifest["date_from"], date_to=manifest["date_to"],
                original_at=datetime.combine(date.fromisoformat(manifest["date_from"]), time(), HK).isoformat(),
                expires_at=release["expires_at"], collector=snapshot["collector"], input=manifest["input"], snapshot_id=snapshot["snapshot_id"])


def validate_metadata(value, config, run_id, *, now=None):
    current = now or datetime.now(UTC)
    paths(config["lane"], run_id)
    if (not isinstance(value, dict) or set(value) != FIELDS or type(value["schema_version"]) is not int or value["schema_version"] != 1
            or value["source"] != "real" or value["kind"] != "aggregate_pair" or value["lane"] != config["lane"]
            or value["run_id"] != run_id or value["file"] != "pair.json"
            or type(value["bytes"]) is not int or not 1 <= value["bytes"] <= MAX_BYTES
            or not re.fullmatch(r"[a-f0-9]{64}", value["sha256"]) or not re.fullmatch(r"[a-f0-9]{64}", value["snapshot_id"])):
        raise ValueError("Unexpected aggregate transfer metadata")
    collector_identity(value["collector"])
    prefix = "hdfs://" + config["nodes"]["snow-control"] + ":9000/snow/ods/real/kafka/" + config["lane"]
    if value["input"] != prefix + "/snapshots/" + value["snapshot_id"] + "/_snapshot.json":
        raise ValueError("Aggregate transfer input escaped its collector lane")
    origin = datetime.combine(date.fromisoformat(value["date_from"]), time(), HK)
    end = date.fromisoformat(value["date_to"])
    if (not origin.date() <= end or (end - origin.date()).days > 365 or
            timestamp(value["original_at"]) != origin or timestamp(value["expires_at"]) != origin + timedelta(days=90)
            or not origin <= current < timestamp(value["expires_at"])):
        raise ValueError("A transfer cannot renew or extend the original aggregate lifetime")
    return value


def validate_payload(payload, manifest, config, run_id, *, now=None):
    current = now or datetime.now(UTC)
    validate_metadata(manifest, config, run_id, now=current)
    if len(payload) != manifest["bytes"] or digest(payload) != manifest["sha256"]:
        raise ValueError("Aggregate transfer checksum/size differs")
    release = json.loads(payload)
    if (not isinstance(release, dict) or type(release.get("schema_version")) is not int or
            release != real_release(release["daily"], release["behavior"], run_id)
            or metadata(release, payload, config["lane"], run_id) != manifest
            or timestamp(release["daily"]["manifest"]["cutoff"]) > current):
        raise ValueError("Only the exact validated aggregate pair can be transferred")
    return release


def reserve(root, config, run_id, manifest, *, now=None):
    current = now or datetime.now(UTC)
    directory = Path(root) / paths(config["lane"], run_id)["directory"]
    if directory.resolve() != directory.absolute():
        raise ValueError("Transfer directory cannot traverse a link")
    existing = RealLifecycle(directory / "data")
    if existing.owner.exists():
        existing.cleanup(current)
    validate_metadata(manifest, config, run_id, now=current)
    with publication_lock(directory):
        immutable = directory / "manifest.json"
        if immutable.exists() and json.loads(read(immutable, 65536)) != manifest:
            raise ValueError("A transfer run cannot replace its immutable metadata")
        lifecycle = RealLifecycle(directory / "data")
        if not lifecycle.owner.exists():
            lifecycle.initialize()
        lifecycle.cleanup(current)
        for name in ("pair.json", "pair.json.tmp"):
            lifecycle.register(name, "aggregate", manifest["original_at"], now=current)
        write_json(immutable, manifest)
        lifecycle.cleanup(current)
    return lifecycle


def export(root, config, run_id, *, now=None):
    current = now or datetime.now(UTC)
    cleanup_copies(root, now=current)
    release = read_real_release(Path(root) / "runtime/real/publication", current)
    if release["run_id"] != run_id:
        raise ValueError("Export only the explicitly requested managed release")
    # Deterministic bytes mean a retry cannot replace an immutable same-run copy.
    from .publication import canonical
    payload = canonical(release)
    manifest = metadata(release, payload, config["lane"], run_id)
    validate_payload(payload, manifest, config, run_id, now=current)
    lifecycle = reserve(root, config, run_id, manifest, now=current)
    target = lifecycle.path("pair.json")
    if target.exists() and read(target) != payload:
        raise ValueError("An immutable export already contains different bytes")
    atomic_write(target, payload)
    lifecycle.cleanup(current)
    return manifest


def accept(root, config, run_id, *, publish=False, now=None):
    current = now or datetime.now(UTC)
    relative = paths(config["lane"], run_id)
    directory = Path(root) / relative["directory"]
    lifecycle = RealLifecycle(directory / "data")
    lifecycle.cleanup(current)
    manifest = validate_metadata(json.loads(read(directory / "manifest.json", 65536)), config, run_id, now=current)
    incoming = lifecycle.readable("pair.json.tmp", current)
    payload = read(incoming)
    release = validate_payload(payload, manifest, config, run_id, now=current)
    target = lifecycle.path("pair.json")
    if target.exists() and read(target) != payload:
        raise ValueError("An immutable destination already contains different bytes")
    atomic_write(target, payload)
    incoming.unlink(missing_ok=True)  # atomic_write normally consumed this exact registered temporary.
    lifecycle.cleanup(current)
    if publish:
        destination = Path(root) / relative["published"]
        result = release_real(release["daily"], release["behavior"], destination, run_id, now=current)
        if result != release or read_real_release(destination, current) != release:
            raise ValueError("Analysis aggregate readback differs from the original pair")
    return manifest


def cleanup_copies(root, *, now=None):
    """Only enumerated transfer roots; never prune a shared host or VM snapshot."""
    current = now or datetime.now(UTC)
    directory = Path(root) / "runtime/real/transfers"
    if not directory.exists():
        return 0
    if directory.resolve() != directory.absolute():
        raise ValueError("Transfer root cannot traverse a link")
    removed = 0
    for lane in directory.iterdir():
        if not lane.is_dir() or lane.resolve() != lane.absolute() or not re.fullmatch(r"[a-z][a-z0-9-]{2,23}", lane.name):
            raise ValueError("Unregistered directory in transfer inventory")
        for run in lane.iterdir():
            paths(lane.name, run.name)
            if not run.is_dir() or run.resolve() != run.absolute():
                raise ValueError("Transfer run is not an owned directory")
            for folder in (run / "data", run / "published/data"):
                lifecycle = RealLifecycle(folder)
                if folder.exists():
                    if not lifecycle.owner.exists():
                        raise ValueError("Transfer payload has no lifecycle owner")
                    removed += lifecycle.cleanup(current)["removed"]
    return removed
