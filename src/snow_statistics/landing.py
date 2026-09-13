"""Single-writer, bounded Kafka archives -> immutable HDFS inputs -> Kafka ACK.

Capture, land and acknowledge may run in separate resource phases. The pending
journal survives all three; Kafka is never advanced by capture or landing.
"""
import base64
import json
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path

from snow_statistics.contracts import Event
from snow_statistics.io import atomic_write, digest, write_json
from snow_statistics.publication import canonical, publication_lock

TOPICS = ("snow.synthetic.events.v1", *(f"snow.synthetic.cdc.snow_ops.{t}" for t in ("campaigns", "contents", "tickets")))
FILES = ("raw.jsonl", "events.jsonl", "changes.jsonl", "quarantine.jsonl")


def topics_for(provenance, event_lane=None):
    if provenance not in {"real", "synthetic"}:
        raise ValueError("Invalid source")
    if event_lane is not None and (provenance != "real" or not re.fullmatch(r"[a-z0-9_]{1,24}", event_lane)):
        raise ValueError("Only an explicit real replay lane may select a separate event topic")
    return TOPICS if provenance == "synthetic" else ("snow.real." + (event_lane + "." if event_lane else "") + "events.v1",)


def load(path, default=None):
    return json.loads(path.read_bytes()) if path.exists() else default


def checked_receipt(receipt):
    try:
        keys = ("schema_version", "source", "identity", "offsets", "batches", "root")
        if receipt["schema_version"] == 2:
            keys += ("head_batch_id",)
        snapshot = {k: receipt[k] for k in keys}
        token = digest(canonical(snapshot))
        head = snapshot.get("head_batch_id") if receipt["schema_version"] == 2 else snapshot["batches"][-1]["batch_id"]
        if (token != receipt["snapshot_id"] or receipt["batch_id"] != head or
                receipt["input"] != snapshot["root"] + "/snapshots/" + token + "/_snapshot.json"):
            raise ValueError("Receipt/checkpoint checksum mismatch")
        return snapshot
    except (KeyError, IndexError, TypeError) as exc:
        raise ValueError("Invalid receipt/checkpoint") from exc


def normalize(record, provenance="synthetic", event_lane=None):
    """Raw key/value bytes (including tombstones) remain in the separate raw ODS."""
    topic = record["topic"]
    topics = topics_for(provenance, event_lane)
    if topic not in topics:
        raise ValueError("Unexpected topic")
    value = record["value_b64"]
    if value is None:
        if topic == topics[0]:
            raise ValueError("Event tombstone is not a v1 event")
        return "tombstones", None
    payload = json.loads(base64.b64decode(value, validate=True))
    position = dict(kafka_topic=topic, kafka_partition=record["partition"], kafka_offset=record["offset"])
    if topic == topics[0]:
        if set(payload) != {"seq", "source", "accepted_at", "event"} or payload["source"] != provenance:
            raise ValueError("Invalid event envelope")
        if type(payload["seq"]) is not int or payload["seq"] < 1:
            raise ValueError("Invalid collector position")
        if not isinstance(payload["accepted_at"], str) or datetime.fromisoformat(payload["accepted_at"].replace("Z", "+00:00")).tzinfo is None:
            raise ValueError("Accepted timestamp requires timezone")
        Event.model_validate(payload["event"])
        return "events", payload | position
    if payload["op"] not in ("r", "c", "u", "d"):
        raise ValueError("Unknown CDC operation")
    table = topic.rsplit(".", 1)[1]
    if payload["source"]["table"] != table or payload["source"]["db"] != "snow_ops":
        raise ValueError("Unexpected CDC source")
    row = payload["before"] if payload["op"] == "d" else payload["after"]
    if not row or type(row["version"]) is not int or row["version"] < 1:
        raise ValueError("Missing CDC row/version")
    stamp = row["updated_at"]
    if type(stamp) not in (int, str):
        raise ValueError("Invalid CDC timestamp")
    stamp = datetime.fromtimestamp(stamp / 1000, UTC) if isinstance(stamp, int) else datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    if stamp.tzinfo is None:
        raise ValueError("CDC timestamp requires timezone")
    return "changes", dict(source="synthetic", table=table, key=row["id"], at=stamp.isoformat(),
                           version=row["version"], op="d" if payload["op"] == "d" else "u",
                           after=payload["after"], transaction=payload.get("transaction"),
                           source_position=payload["source"], **position)


def pending(directory):
    token = load(directory / "pending.json")
    if token is None:
        raise ValueError("No pending capture")
    manifest = load(directory / "batches" / token["batch_id"] / "manifest.json")
    if digest(canonical(manifest)) != token["batch_id"]:
        raise ValueError("Archive manifest checksum mismatch")
    for name, checksum in manifest["files"].items():
        if name not in FILES or digest((directory / "batches" / token["batch_id"] / name).read_bytes()) != checksum:
            raise ValueError("Archive payload checksum mismatch")
    return token["batch_id"], manifest


def capture(directory, source, max_records=10000, max_bytes=32 * 1024**2, *, provenance="synthetic", now=None, event_lane=None):
    """Source interface: identity, bounds, committed, read(start, end), commit."""
    directory = Path(directory)
    topics = topics_for(provenance, event_lane)
    if not 1 <= max_records <= 100000 or not 1 <= max_bytes <= 64 * 1024**2:
        raise ValueError("Invalid bounded capture limits")
    with publication_lock(directory):
        if (directory / "cleanup.json").exists():
            raise ValueError("Incomplete real cleanup blocks capture")
        if (directory / "pending.json").exists():
            return pending(directory)[0]  # Always recover before polling newer records.
        state = load(directory / "state.json", {})
        if state:
            checked_receipt(state)
        identity = source.identity()
        if set(identity["topic_ids"]) != set(topics):
            raise ValueError("Kafka topics do not match requested provenance")
        if state and state["source"] != provenance:
            raise ValueError("Cannot reuse a landing directory for another source")
        if state and state["identity"] != identity:
            raise ValueError("Kafka cluster/topic incarnation or consumer group changed")
        bounds = source.bounds()
        previous = state.get("offsets", {})
        if previous and set(previous) != set(bounds):
            raise ValueError("Partition topology changed; explicit migration required")
        starts, ends = {}, {}
        for key, (earliest, end) in sorted(bounds.items()):
            start = previous.get(key, 0)
            if not earliest <= start <= end:
                raise ValueError("Kafka retention gap or offset regression; refusing reset")
            committed = source.committed(key)
            if committed != (start if state else None):
                raise ValueError("Consumer group offset differs from the owned journal")
            starts[key], ends[key] = start, end
        total = sum(ends[k] - starts[k] for k in starts)
        if total == 0:
            return None
        # Bound each capture without starving later partitions: round-robin quota.
        budget = max_records
        for i, key in enumerate(starts):
            take = min(ends[key] - starts[key], max(1, budget // (len(starts) - i))) if budget else 0
            ends[key] = starts[key] + take
            budget -= take
        groups = {name: [] for name in ("raw", "events", "changes", "quarantine")}
        tombstones, size = 0, 0
        for key in starts:
            expected = starts[key]
            for record in source.read(key, starts[key], ends[key]):
                if record["offset"] != expected or f'{record["topic"]}:{record["partition"]}' != key or expected >= ends[key]:
                    raise ValueError("Missing, duplicate or out-of-range Kafka position")
                expected += 1
                size += len(canonical(record))
                if size > max_bytes:
                    raise ValueError("Capture byte budget exceeded; reduce max_records")
                groups["raw"].append(record)
                try:
                    kind, normalized = normalize(record, provenance, event_lane)
                    if kind == "tombstones":
                        tombstones += 1
                    else:
                        groups[kind].append(normalized)
                except (ValueError, TypeError, KeyError, OverflowError):
                    groups["quarantine"].append(dict(topic=record["topic"], partition=record["partition"],
                                                     offset=record["offset"], reason="invalid_contract"))
            if expected != ends[key]:
                raise ValueError("Incomplete Kafka range; offsets unchanged")
        if source.identity() != identity:
            raise ValueError("Kafka identity changed during capture")
        contents = {name + ".jsonl": b"".join(canonical(r) + b"\n" for r in rows) for name, rows in groups.items()}
        manifest = dict(schema_version=1, source=provenance, identity=identity, starts=starts, ends=ends,
                        parent=state.get("snapshot_id"), files={n: digest(v) for n, v in contents.items()},
                        counts={name: len(rows) for name, rows in groups.items()} | {"tombstones": tombstones})
        if provenance == "real":
            # Unknown payloads cannot be assigned a trustworthy original expiry.
            # Refuse before archiving or committing; retain metadata-only evidence.
            if groups["quarantine"] or not groups["events"]:
                raise ValueError("Invalid real input; raw capture refused before persistence")
            accepted = [datetime.fromisoformat(r["accepted_at"].replace("Z", "+00:00")) for r in groups["events"]]
            current = now or datetime.now(UTC)
            if min(accepted) + timedelta(days=7) <= current or max(accepted) > current:
                raise ValueError("Real input is expired or future-dated; explicit gap recovery required")
            manifest["original_min_accepted_at"] = min(accepted).isoformat()
            manifest["expires_at"] = (min(accepted) + timedelta(days=7)).isoformat()
        batch = digest(canonical(manifest))
        folder = directory / "batches" / batch
        for name, body in contents.items():
            immutable(folder / name, body)
        immutable(folder / "manifest.json", canonical(manifest))
        write_json(directory / "pending.json", {"batch_id": batch})
        return batch


def immutable(path, body):
    if path.exists():
        if path.read_bytes() != body:
            raise ValueError("Immutable local archive conflicts with existing content")
    else:
        atomic_write(path, body)


def land(directory, sink, fail_after_batch=False, *, now=None):
    directory = Path(directory)
    with publication_lock(directory):
        batch, manifest = pending(directory)
        if "/snow/ods/" + manifest["source"] + "/kafka/" not in sink.root:
            raise ValueError("Landing source and HDFS destination differ")
        if manifest["source"] == "real" and datetime.fromisoformat(manifest["expires_at"]) <= (now or datetime.now(UTC)):
            raise ValueError("Real pending input expired before HDFS publication")
        state = load(directory / "state.json", {})
        if state:
            checked_receipt(state)
        already_checkpointed = state.get("batch_id") == batch
        if not already_checkpointed and manifest["parent"] != state.get("snapshot_id"):
            raise ValueError("Pending capture belongs to a different parent")
        if state and state["root"] != sink.root:
            raise ValueError("HDFS destination changed; explicit migration required")
        files = {name: (directory / "batches" / batch / name).read_bytes() for name in FILES}
        files["manifest.json"] = canonical(manifest)
        sink.put_directory("batches/" + batch, files)
        if fail_after_batch:
            raise RuntimeError("Injected failure after HDFS batch commit, before snapshot commit")
        entry = dict(batch_id=batch, files=manifest["files"], counts=manifest["counts"])
        if manifest["source"] == "real":
            entry.update({k: manifest[k] for k in ("original_min_accepted_at", "expires_at")})
        entries = state["batches"] if already_checkpointed else state.get("batches", []) + [entry]
        snapshot = dict(schema_version=1, source=manifest["source"], identity=manifest["identity"], offsets=manifest["ends"],
                        batches=entries, root=sink.root)
        if manifest["source"] == "real":
            snapshot.update(schema_version=2, head_batch_id=batch)
        snapshot_id = digest(canonical(snapshot))
        sink.put_directory("snapshots/" + snapshot_id, {"_snapshot.json": canonical(snapshot)})
        receipt = snapshot | {"snapshot_id": snapshot_id, "batch_id": batch,
                              "input": sink.root + "/snapshots/" + snapshot_id + "/_snapshot.json"}
        write_json(directory / "landed.json", receipt)
        if manifest["source"] == "real":
            write_json(directory / "receipts" / (snapshot_id + ".json"), receipt)
        return receipt


def acknowledge(directory, source, fail_after_commit=False):
    directory = Path(directory)
    with publication_lock(directory):
        batch, manifest = pending(directory)
        receipt = load(directory / "landed.json", {})
        if receipt.get("batch_id") != batch or receipt.get("offsets") != manifest["ends"]:
            raise ValueError("HDFS snapshot has not been verified; refusing Kafka ACK")
        checked_receipt(receipt)
        if source.identity() != manifest["identity"]:
            raise ValueError("Receipt corruption or changed Kafka identity")
        state = load(directory / "state.json", {})
        if state:
            checked_receipt(state)
        initial = not state or (state.get("snapshot_id") == receipt["snapshot_id"] and manifest["parent"] is None)
        for key, end in manifest["ends"].items():
            allowed = {None if initial else manifest["starts"][key], end}
            if source.committed(key) not in allowed:
                raise ValueError("Consumer group changed during staged landing")
        source.commit(manifest["ends"])
        if fail_after_commit:
            raise RuntimeError("Injected failure after Kafka ACK, before local checkpoint")
        write_json(directory / "state.json", receipt)
        (directory / "pending.json").unlink()
        return receipt
