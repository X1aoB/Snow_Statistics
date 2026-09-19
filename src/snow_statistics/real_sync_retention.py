"""Startup cleanup for an existing, bound real epoch's local sync registry.

This never imports external files, creates a lifecycle registry, reads payloads,
or renews expiry. Collector identity is checked against the immutable writer
registration; an epoch generation is not a collector generation.
"""
import json
import stat
from pathlib import Path
from urllib.parse import urlsplit
from uuid import UUID

from .io import digest
from .landing import collector_identity
from .lifecycle import RealLifecycle
from .publication import canonical, publication_lock


def _directory(path):
    """Absence is allowed, but a dangling link or non-directory is not absence."""
    if path.resolve() != path.absolute():
        raise ValueError("Sync cleanup directory traverses a link")
    try:
        info = path.lstat()
    except FileNotFoundError:
        return False
    if not stat.S_ISDIR(info.st_mode):
        raise ValueError("Sync cleanup requires an owned directory")
    return True


def _regular(path, *, optional=False, limit=262144):
    if path.resolve() != path.absolute():
        raise ValueError("Sync cleanup metadata traverses a link")
    try:
        info = path.lstat()
    except FileNotFoundError:
        if optional:
            return False
        raise ValueError("Existing sync data is missing ownership metadata") from None
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > limit:
        raise ValueError("Sync cleanup metadata is not a bounded unlinked regular file")
    return True


def _metadata(path, *, limit=262144):
    _regular(path, limit=limit)
    return json.loads(path.read_bytes())


def cleanup_epoch_sync(epoch_directory, manifest):
    """Caller holds no epoch lock; acquire sync -> data, then release both.

    The epoch startup/watch caller subsequently takes any retirement lock.
    Expired writer admission is deliberately not required to remove expired
    data. Only its immutable identity metadata is used; no engine read is made.
    """
    from .real_epoch import verify_manifest

    verify_manifest(manifest)
    if manifest["mode"] != "engines" or manifest["input_origin"] != "real":
        return None
    epoch_directory = Path(epoch_directory).absolute()
    if (epoch_directory.name != manifest["epoch_id"] or epoch_directory.parent.name != "epochs"
            or not _directory(epoch_directory)):
        raise ValueError("Sync cleanup must be anchored to its actual epoch directory")
    actual_manifest = _metadata(epoch_directory / "manifest.json")
    if actual_manifest != manifest:
        raise ValueError("Actual epoch manifest changed before sync cleanup")
    sync_root = epoch_directory.parent.parent / "sync"
    directory = sync_root / manifest["epoch_id"]
    data = directory / "data"
    if not _directory(sync_root) or not _directory(directory) or not _directory(data):
        return None
    _regular(directory / "publisher.lock", optional=True, limit=16)
    # Same outer lock and order as sync_once; do not hold the epoch retirement
    # lock while waiting for an active producer to finish its current batch.
    with publication_lock(directory):
        for path in (sync_root, directory, data):
            if not _directory(path):
                raise ValueError("Owned sync directory disappeared during admission")
        target = _metadata(directory / "target.json", limit=65536)
        if (not isinstance(target, dict) or set(target) != {"schema_version", "url", "bootstrap", "lane", "source"}
                or type(target["schema_version"]) is not int or target["schema_version"] != 1
                or target["source"] != "real" or target["lane"] != manifest["event_lane"]
                or not isinstance(target["url"], str) or not isinstance(target["bootstrap"], str)
                or not target["bootstrap"] or any(c.isspace() for c in target["bootstrap"])):
            raise ValueError("Sync target is not bound to this real epoch lane")
        endpoint = urlsplit(target["url"])
        if (endpoint.scheme not in {"http", "https"} or not endpoint.netloc or endpoint.username
                or endpoint.password or endpoint.query or endpoint.fragment):
            raise ValueError("Sync target metadata contains an invalid service endpoint")
        source = collector_identity(_metadata(directory / "source.json", limit=65536))
        registration = _metadata(epoch_directory / "writer-registration.json")
        expected = dict(schema_version=1, source="real", input_origin="real", epoch_id=manifest["epoch_id"],
                        epoch_generation=manifest["generation"], owner_manifest_sha256=digest(canonical(manifest)),
                        original_min_accepted_at=manifest["original_min_accepted_at"], expires_at=manifest["expires_at"])
        if (not isinstance(registration, dict) or type(registration.get("schema_version")) is not int
                or any(registration.get(key) != value for key, value in expected.items())
                or collector_identity(registration.get("collector")) != source):
            raise ValueError("Sync collector and immutable epoch registration differ")
        _regular(data / ".snow-real-owner.json", limit=65536)
        _regular(data / "registry.json", limit=8 * 1024**2)
        for filename in ("publisher.lock", "gate.json"):
            _regular(data / filename, optional=True)
        owner = _metadata(data / ".snow-real-owner.json", limit=65536)
        if (not isinstance(owner, dict) or set(owner) != {"project", "source", "instance_id"}
                or owner["project"] != "snow-statistics" or owner["source"] != "real"
                or not isinstance(owner["instance_id"], str) or str(UUID(owner["instance_id"])) != owner["instance_id"]):
            raise ValueError("Existing sync lifecycle owner is invalid")
        # RealLifecycle performs complete inventory and preserves every original
        # per-artifact expiry; unknown physical payload keeps the gate closed.
        return RealLifecycle(data).cleanup()
