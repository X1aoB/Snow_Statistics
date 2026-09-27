"""Fail-closed ownership and expiry of local real-data artifacts.

This registry is a prerequisite for the real runner, not a claim that Kafka,
HDFS or database copies disappear automatically. Each remote backend must return
a verified purge receipt before the runner opens readers. No shell is executed.
"""
import json
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

from .io import write_json
from .publication import publication_lock

RETENTION_DAYS = {"raw": 7, "request_detail": 7, "auxiliary": 30, "aggregate": 90}
CONTROL_FILES = {".snow-real-owner.json", "registry.json", "publisher.lock", "gate.json"}


def timestamp(value):
    at = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if at.tzinfo is None:
        raise ValueError("Lifecycle timestamps require an explicit timezone")
    return at.astimezone(UTC)


class RealLifecycle:
    def __init__(self, root):
        self.root = Path(root).absolute()
        self.owner = self.root / ".snow-real-owner.json"
        self.registry = self.root / "registry.json"

    def initialize(self):
        if self.root.exists() and any(self.root.iterdir()):
            raise ValueError("Initialize only an empty dedicated real-data root")
        self.root.mkdir(parents=True, exist_ok=True)
        if self.root.resolve() != self.root:
            raise ValueError("Real root cannot traverse symbolic links")
        write_json(self.owner, dict(project="snow-statistics", source="real", instance_id=str(uuid4())))
        write_json(self.registry, dict(schema_version=1, artifacts={}))

    def _read(self):
        owner = json.loads(self.owner.read_bytes())
        if owner.get("project") != "snow-statistics" or owner.get("source") != "real":
            raise ValueError("Missing real-data ownership marker")
        if self.root.resolve() != self.root:
            raise ValueError("Real root now resolves outside its registered path")
        data = json.loads(self.registry.read_bytes())
        if data.get("schema_version") != 1 or not isinstance(data.get("artifacts"), dict):
            raise ValueError("Invalid lifecycle registry")
        return data

    def path(self, relative):
        part = Path(relative)
        if (part.is_absolute() or not part.parts or any(p in {".", ".."} for p in part.parts)
                or part.parts[0] in CONTROL_FILES or ":" in relative or "\\" in relative):
            raise ValueError("Unsafe real artifact path")
        result = self.root / part
        if result.resolve() != result.absolute() or not result.resolve().is_relative_to(self.root.resolve()):
            raise ValueError("Artifact resolves outside owned root or through a link")
        # Reject directory links too, before recursive removal can encounter them.
        if result.is_dir():
            for child in result.rglob("*"):
                if child.resolve() != child.absolute():
                    raise ValueError("Artifact contains a link")
        return result

    def register(self, relative, kind, original_at, *, references=(), copied_from=None, now=None):
        """Reserve BEFORE writing payload; crash cleanup can then still discover it.

        original_at is earliest original acceptance in a file/checkpoint, not its
        modification or backup time. Whole-file expiry may be conservative.
        references are physical file dependencies (not general lineage edges).
        """
        if kind not in RETENTION_DAYS:
            raise ValueError("Unknown real data retention class")
        current = now or datetime.now(UTC)
        origin = timestamp(original_at)
        if origin > current:
            raise ValueError("Original time cannot be in the future")
        expires = origin + timedelta(days=RETENTION_DAYS[kind])
        if expires <= current:
            raise ValueError("Refuse to import already expired data")
        self.path(relative)
        with publication_lock(self.root):
            data = self._read()
            entries = data["artifacts"]
            if relative in entries:
                previous = entries[relative]
                if (previous["kind"] == kind and timestamp(previous["original_at"]) == origin
                        and previous["references"] == list(references) and previous["copied_from"] == copied_from):
                    return self.path(relative)  # Replay a reservation after a crash; never extend expiry.
                raise ValueError("Artifact already registered; use a new immutable path")
            if any(relative.startswith(k + "/") or k.startswith(relative + "/") for k in entries):
                raise ValueError("Overlapping retention roots are ambiguous")
            if any(k not in entries for k in references):
                raise ValueError("Unregistered physical dependency")
            if copied_from is not None:
                parent = entries[copied_from]
                if parent["kind"] != kind or timestamp(parent["original_at"]) != origin:
                    raise ValueError("Copy/restore cannot reset original retention time")
                expires = min(expires, timestamp(parent["expires_at"]))
            entries[relative] = dict(kind=kind, original_at=origin.isoformat(), expires_at=expires.isoformat(),
                                     references=list(references), copied_from=copied_from)
            write_json(self.registry, data)
            write_json(self.root / "gate.json", dict(open=False, reason="registration_changed"))
        return self.path(relative)

    def _plan(self, data, now):
        entries = data["artifacts"]
        expired = set()
        for key, entry in entries.items():
            self.path(key)
            if entry["kind"] not in RETENTION_DAYS:
                raise ValueError("Unknown registered retention class")
            if timestamp(entry["expires_at"]) > timestamp(entry["original_at"]) + timedelta(days=RETENTION_DAYS[entry["kind"]]):
                raise ValueError("Registry tries to extend retention")
            if any(ref not in entries for ref in entry["references"]):
                raise ValueError("Dangling physical dependency")
            if timestamp(entry["expires_at"]) <= now:
                expired.add(key)
        # A snapshot that points to an expired batch cannot be offered to readers.
        while True:
            extra = {k for k, e in entries.items() if set(e["references"]) & expired}
            if extra <= expired:
                break
            expired |= extra
        return sorted(expired)

    def _inventory(self, data):
        registered = set(data["artifacts"])
        for file in self.root.rglob("*"):
            self.path(file.relative_to(self.root).as_posix()) if file.name not in CONTROL_FILES else None
            if file.is_dir():
                continue
            relative = file.relative_to(self.root).as_posix()
            if relative in CONTROL_FILES:
                continue
            if not any(relative == key or relative.startswith(key + "/") for key in registered):
                raise ValueError("Unregistered real payload blocks startup")

    def plan(self, now=None):
        data = self._read()
        self._inventory(data)
        return self._plan(data, now or datetime.now(UTC))

    def cleanup(self, now=None):
        """Do not launch readers until successful completion. Partial failure stays shut."""
        current = now or datetime.now(UTC)
        with publication_lock(self.root):
            write_json(self.root / "gate.json", dict(open=False, reason="cleanup_in_progress"))
            data = self._read()
            self._inventory(data)
            expired = self._plan(data, current)
            for relative in expired:
                target = self.path(relative)
                if target.is_dir():
                    shutil.rmtree(target)
                else:
                    target.unlink(missing_ok=True)
            for relative in expired:
                del data["artifacts"][relative]
            write_json(self.registry, data)
            gate = dict(open=True, checked_at=current.isoformat(), removed=len(expired),
                        next_expiry=min((e["expires_at"] for e in data["artifacts"].values()), default=None))
            write_json(self.root / "gate.json", gate)
            return gate

    def readable(self, relative, now=None):
        data = self._read()
        gate = json.loads((self.root / "gate.json").read_bytes())
        current = now or datetime.now(UTC)
        if not gate["open"] or self._plan(data, current):
            raise ValueError("Cleanup required before real-data reads")
        if relative not in data["artifacts"]:
            raise ValueError("Unregistered read target")
        self._inventory(data)
        return self.path(relative)
