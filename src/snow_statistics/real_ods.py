"""Expire the real ODS data window while preserving its durable Kafka head.

Run with readers/consumers stopped. HDFS snapshots here are application manifests,
not native HDFS snapshots. A cleanup journal blocks advancement on failure.
"""
import json
import re
import shutil
from datetime import UTC, datetime
from pathlib import Path

from .io import digest, write_json
from .landing import checked_receipt, load
from .publication import canonical, publication_lock


def expire_window(directory, sink, *, now=None):
    directory = Path(directory)
    current = now or datetime.now(UTC)
    with publication_lock(directory):
        state = load(directory / "state.json", {})
        if not state:
            return dict(removed=0, source="real")
        snapshot = checked_receipt(state)
        if snapshot["schema_version"] != 2 or snapshot["source"] != "real" or snapshot["root"] != sink.root:
            raise ValueError("Only registered v2 real ODS can be expired")
        if (directory / "pending.json").exists():
            raise ValueError("Resolve pending capture before expiring the committed window")
        journal = directory / "cleanup.json"
        plan = load(journal)
        if plan is not None and state["snapshot_id"] == digest(canonical(plan["replacement"])):
            # Crash after durable state commit: all payload deletes preceded it.
            write_json(directory / "landed.json", state)
            for receipt_path in directory.glob("receipts/*.json"):
                old = checked_receipt(json.loads(receipt_path.read_bytes()))
                if any(e["batch_id"] in plan["expired"] for e in old["batches"]):
                    receipt_path.unlink()
            write_json(directory / "receipts" / (state["snapshot_id"] + ".json"), state)
            journal.unlink()
            return dict(removed=len(plan["expired"]), source="real", recovered=True,
                        offsets=state["offsets"], window_batches=len(state["batches"]))
        expired = [e for e in snapshot["batches"] if datetime.fromisoformat(e["expires_at"]) <= current]
        if not expired:
            return dict(removed=0, source="real")
        expired_ids = {e["batch_id"] for e in expired}
        # Metadata-only frozen journal makes partial deletion retryable. Do not
        # replace the old cursor/head until all backend deletes have been verified.
        if plan is None:
            plan = dict(old_snapshot_id=state["snapshot_id"], expired=sorted(expired_ids),
                        replacement=snapshot | {"batches": [e for e in snapshot["batches"] if e["batch_id"] not in expired_ids]})
            write_json(journal, plan)
        elif plan["old_snapshot_id"] != state["snapshot_id"]:
            raise ValueError("Cleanup journal no longer matches ODS head")
        expired_ids = set(plan["expired"])
        replacement = plan["replacement"]
        token = digest(canonical(replacement))
        # First write a valid successor with no expired references.
        sink.put_directory("snapshots/" + token, {"_snapshot.json": canonical(replacement)})
        old_snapshots = set()
        for receipt_path in directory.glob("receipts/*.json"):
            receipt = json.loads(receipt_path.read_bytes())
            old = checked_receipt(receipt)
            if old["root"] != sink.root or old["source"] != "real":
                raise ValueError("Foreign receipt in real ODS registry")
            if any(e["batch_id"] in expired_ids for e in old["batches"]):
                old_snapshots.add(receipt["snapshot_id"])
        old_snapshots.add(state["snapshot_id"])
        for old in old_snapshots:
            sink.delete_real_directory("snapshots/" + old)
        for batch in plan["expired"]:
            if not re.fullmatch(r"[0-9a-f]{64}", batch):
                raise ValueError("Unsafe raw batch identifier")
            sink.delete_real_directory("batches/" + batch)
            local = directory / "batches" / batch
            resolved = local.resolve()
            if resolved != local.absolute() or not resolved.is_relative_to(directory.resolve()):
                raise ValueError("Local batch does not resolve inside real lane")
            if local.exists():
                if any(p.resolve() != p.absolute() for p in local.rglob("*")):
                    raise ValueError("Linked file blocks raw batch deletion")
                shutil.rmtree(local)
        receipt = replacement | dict(snapshot_id=token, batch_id=replacement["head_batch_id"],
                                     input=sink.root + "/snapshots/" + token + "/_snapshot.json")
        checked_receipt(receipt)
        write_json(directory / "state.json", receipt)
        write_json(directory / "landed.json", receipt)
        for old in old_snapshots:
            (directory / "receipts" / (old + ".json")).unlink(missing_ok=True)
        write_json(directory / "receipts" / (token + ".json"), receipt)
        journal.unlink()
        return dict(removed=len(plan["expired"]), source="real", snapshot_id=token,
                    offsets=receipt["offsets"], window_batches=len(receipt["batches"]))
