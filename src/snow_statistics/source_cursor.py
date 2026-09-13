"""Collector generation binding and explicit, metadata-only gap receipts.

The caller holds the sync writer lock. A new database behind the same URL is
never the same stream; a retained offset is not proof that the data still exists.
"""
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit
from uuid import UUID, uuid4

from .io import write_json


class SourceGap(ValueError):
    pass


def validate_status(status):
    if status.get("schema_version") != 1 or status.get("source") not in {"real", "synthetic"}:
        raise ValueError("Invalid collector status")
    for name in ("instance_id", "generation"):
        UUID(status[name])
    for name in ("latest_accepted_seq", "expired_through", "aggregate_cursor"):
        if type(status.get(name)) is not int or status[name] < 0:
            raise ValueError("Invalid collector cursor bounds")
    earliest = status.get("earliest_available_seq")
    latest = status["latest_accepted_seq"]
    expired = status["expired_through"]
    if not expired <= latest or not status["aggregate_cursor"] <= latest:
        raise ValueError("Inconsistent collector bounds")
    if earliest is not None and (type(earliest) is not int or not expired < earliest <= latest):
        raise ValueError("Invalid earliest readable cursor")
    return {k: status[k] for k in ("schema_version", "source", "instance_id", "generation")}


def record_gap(directory, reason, after, status=None):
    """No raw events, endpoint, credentials, request IDs or error bodies."""
    gap = dict(schema_version=1, reason=reason, after=after,
               detected_at=datetime.now(UTC).isoformat())
    if status is not None:
        validate_status(status)
        gap["observed"] = {k: status[k] for k in (
            "source", "instance_id", "generation", "earliest_available_seq",
            "latest_accepted_seq", "expired_through")}
    write_json(Path(directory) / "gaps" / (uuid4().hex + ".json"), gap)
    write_json(Path(directory) / "gap.json", gap)
    return gap


def check_source(directory, status, after, *, expected_source):
    directory = Path(directory)
    identity = validate_status(status)
    if identity["source"] != expected_source:
        raise ValueError("Unexpected source in collector status")
    if (directory / "gap.json").exists():
        raise SourceGap("Unresolved source gap; explicit rebase into a fresh lane required")
    if status.get("aggregate_gap"):
        record_gap(directory, "upstream_aggregate_gap", after, status)
        raise SourceGap("Collector reports lost unaggregated input; explicit recovery required")
    binding = directory / "source.json"
    if binding.exists() and json.loads(binding.read_bytes()) != identity:
        record_gap(directory, "source_generation_changed", after, status)
        raise SourceGap("Collector source generation changed")
    if not binding.exists():
        if after or (directory / "pending.json").exists():
            record_gap(directory, "legacy_unbound_source", after, status)
            raise SourceGap("Legacy archive lacks a source generation; explicit migration required")
        write_json(binding, identity)
    if after < status["expired_through"]:
        record_gap(directory, "retention_gap", after, status)
        raise SourceGap("Collector retention gap; refusing to skip unread data")
    if after > status["latest_accepted_seq"]:
        record_gap(directory, "cursor_regression", after, status)
        raise SourceGap("Collector cursor regressed")
    earliest = status["earliest_available_seq"]
    if after < status["latest_accepted_seq"] and (earliest is None or earliest > after + 1):
        record_gap(directory, "unreadable_range", after, status)
        raise SourceGap("Collector readable range is incomplete")
    return identity


def rebase(directory, destination, status, *, after, reason, identity):
    """Create a NEW lane, preserving the old journal and an explicit loss record.

Caller supplies the new transport identity and current protected status. Reusing
the old lane/topic is forbidden: first-acceptance sequence semantics changed.
"""
    from .publication import publication_lock
    directory, destination = Path(directory), Path(destination)
    if destination.resolve() == directory.resolve() or not reason.strip():
        raise ValueError("Rebase requires a fresh directory and a recorded reason")
    source = validate_status(status)
    if type(after) is not int or not status["expired_through"] <= after <= status["latest_accepted_seq"]:
        raise ValueError("New start must lie inside current source bounds")
    old_target = json.loads((directory / "target.json").read_bytes())
    if identity.get("lane") == old_target.get("lane") or not identity.get("lane"):
        raise ValueError("Use a fresh replay lane when rebasing")
    if identity.get("source") != source["source"]:
        raise ValueError("Source and new target disagree")
    if destination.exists() and any(destination.iterdir()):
        raise ValueError("Rebase destination must be empty")
    with publication_lock(destination):
        write_json(destination / "target.json", identity)
        write_json(destination / "source.json", source)
        write_json(destination / "cursor.json", {"cursor": after})
        # Reason is operator-supplied metadata. Never paste events or credentials.
        receipt = dict(schema_version=1, reason=reason, after=after, source=source,
                       old_lane=old_target.get("lane"), new_lane=identity["lane"],
                       created_at=datetime.now(UTC).isoformat(), coverage="starts_after_explicit_gap")
        write_json(destination / "rebase.json", receipt)
        return receipt


def rebase_from_collector(url, token, bootstrap, directory, destination, *, lane, after, reason):
    """An operator-requested new lane; no automatic recovery from HTTP 410."""
    import httpx
    endpoint = urlsplit(url)
    if (not token or endpoint.scheme not in {"http", "https"} or not endpoint.netloc or endpoint.username or
            endpoint.password or endpoint.query or endpoint.fragment or endpoint.path not in {"", "/"}):
        raise ValueError("Use a collector origin and a separate reader token")
    if not re.fullmatch(r"[a-z0-9_]{1,24}", lane) or not re.fullmatch(r"[a-z0-9_-]{1,100}", reason):
        raise ValueError("Use a fresh lane and bounded metadata reason code")
    with httpx.Client(base_url=url.rstrip("/"), headers={"Authorization": "Bearer " + token},
                      timeout=15, follow_redirects=False, trust_env=False) as client:
        response = client.get("/analytics/private/v1/status")
        response.raise_for_status()
        status = response.json()
    if status.get("source") != "real" or status.get("aggregate_gap"):
        raise ValueError("A healthy exact real collector is required before rebasing")
    identity = dict(schema_version=1, url=url.rstrip("/"), bootstrap=bootstrap, lane=lane, source="real")
    return rebase(directory, destination, status, after=after, reason=reason, identity=identity)
