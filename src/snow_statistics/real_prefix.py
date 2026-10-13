"""Pure metadata checks for an explicitly rebased real input prefix.

Callers obtain a live WriterRegistry registration, check file ownership/lifecycle,
and hash actual archives before calling. This module reads no payload, opens no
service, writes no receipt or permit, and cannot certify payload equivalence,
HDFS replicas or current Kafka acknowledgement from metadata alone.
"""
import re
from datetime import timedelta

from .io import digest
from .landing import FILES, checked_receipt, collector_identity
from .lifecycle import timestamp
from .publication import canonical
from .real_lab import validate_config
from .real_quiescent import bind_ods
from .source_cursor import validate_status

MAX_BATCHES = 256
HEX = re.compile(r"[a-f0-9]{64}")


def exact(value, keys, label):
    if not isinstance(value, dict) or set(value) != set(keys):
        raise ValueError("Unexpected " + label + " fields")


def lifetime(origin, expiry, lower, now):
    start, end = timestamp(origin), timestamp(expiry)
    if not lower <= start <= now < end or end != start + timedelta(days=7):
        raise ValueError("Original acceptance lifetime changed, expired or predates the epoch")
    return start


def validate_rebased_prefix(*, config, registration, source, target, cursor, rebase, status,
                            sync_manifests, lifecycle_artifacts, archive_checksums,
                            ods_state, ods_landed, ods_manifests, now):
    """Check actual metadata schemas; return scope facts, never read permission.

    sync_manifests maps registry names ``batches/<first>-<last>.json`` to the
    existing companion JSON. lifecycle_artifacts is registry.json's artifacts;
    archive_checksums maps those same names to independently observed byte hashes.
    ods_manifests maps actual batch IDs to their manifest.json objects.

    ``status`` is one fixed authenticated observation, not a wall-clock watermark.
    Exact cursor equality deliberately rejects a later/different source prefix.
    All metadata is private: output hashes and sequence bounds do not establish
    public anonymity. No business date is inferred from accepted_at.
    """
    config = validate_config(config)
    if config["input_origin"] != "real":
        raise ValueError("Explicit real input configuration required")
    identity = collector_identity(source)
    if validate_status(status) != identity or registration["collector"] != identity:
        raise ValueError("Collector generation differs across the fixed prefix")
    lane, event_lane = config["lane"], config["lane"].replace("-", "_")
    if registration["epoch_id"] != lane or status.get("aggregate_gap"):
        raise ValueError("Wrong epoch or upstream aggregate gap")
    lower = lifetime(registration["original_min_accepted_at"], registration["expires_at"],
                     timestamp(registration["original_min_accepted_at"]), now)
    expected_target = dict(schema_version=1, url=config["collector_url"].rstrip("/"),
                           bootstrap=config["nodes"]["snow-analysis"] + ":9092", lane=event_lane, source="real")
    if target != expected_target:
        raise ValueError("Original sync transport differs from the registered lane")
    exact(rebase, ("schema_version", "reason", "after", "source", "old_lane", "new_lane", "created_at", "coverage"), "rebase")
    after, through = rebase["after"], status["latest_accepted_seq"]
    if (rebase["schema_version"] != 1 or rebase["source"] != identity
            or rebase["coverage"] != "starts_after_explicit_gap"
            or rebase["new_lane"] != event_lane or rebase["old_lane"] == event_lane
            or not isinstance(rebase["old_lane"], str) or not re.fullmatch(r"[a-z0-9_]{1,24}", rebase["old_lane"])
            or not isinstance(rebase["reason"], str) or not 1 <= len(rebase["reason"].strip()) <= 200
            or type(after) is not int or not 0 <= status["expired_through"] <= after < through
            or status["earliest_available_seq"] is None or status["earliest_available_seq"] > after + 1
            or not timestamp(rebase["created_at"]) <= timestamp(status["generated_at"]) <= now):
        raise ValueError("Rebase does not bind a live explicit source prefix")
    if cursor != {"cursor": through} or type(cursor["cursor"]) is not int:
        raise ValueError("Sync cursor differs from the fixed observed source boundary")
    if not isinstance(sync_manifests, dict) or not 1 <= len(sync_manifests) <= MAX_BATCHES:
        raise ValueError("Bounded original sync manifests required")
    names = set(sync_manifests)
    if set(lifecycle_artifacts) != names | {name + ".manifest.json" for name in names} or set(archive_checksums) != names:
        raise ValueError("Sync lifecycle/hash inventory is incomplete or includes unknown artifacts")
    previous, origins, sync_chain = after, [], []
    for name in sorted(names):
        match = re.fullmatch(r"batches/([0-9]{20})-([0-9]{20})\.json", name)
        if not match:
            raise ValueError("Unexpected original sync archive name")
        first, last = map(int, match.groups())
        manifest, item = sync_manifests[name], lifecycle_artifacts[name]
        exact(manifest, ("archive", "sha256", "after"), "sync manifest")
        exact(item, ("kind", "original_at", "expires_at", "references", "copied_from"), "sync lifetime")
        if (manifest["archive"] != "data/" + name or type(manifest["after"]) is not int
                or manifest["after"] != previous or first != previous + 1 or not first <= last <= through
                or not isinstance(manifest["sha256"], str) or not HEX.fullmatch(manifest["sha256"])
                or archive_checksums[name] != manifest["sha256"]):
            raise ValueError("Sync archive hash or rebased sequence chain differs")
        if (item != lifecycle_artifacts[name + ".manifest.json"] or item["kind"] != "raw"
                or item["references"] != [] or item["copied_from"] is not None):
            raise ValueError("Original sync archive/companion lifetime differs")
        origins.append(lifetime(item["original_at"], item["expires_at"], lower, now))
        sync_chain.append(dict(name=name, manifest=manifest, lifetime=item))
        previous = last
    if previous != through or min(origins) != lower:
        raise ValueError("Sync chain does not reach its boundary or original acceptance origin")

    exact(ods_state, ("schema_version", "source", "identity", "offsets", "batches", "root", "head_batch_id",
                      "snapshot_id", "batch_id", "input"), "ODS state")
    if ods_state != ods_landed:
        raise ValueError("ODS state does not match the acknowledged landed metadata")
    snapshot = checked_receipt(ods_state)
    bind_ods(registration, snapshot)
    topic = "snow.real." + event_lane + ".events.v1"
    key = topic + ":0"
    expected_root = "hdfs://" + config["nodes"]["snow-control"] + ":9000/snow/ods/real/kafka/" + lane
    if (snapshot["schema_version"] != 2 or snapshot["root"] != expected_root
            or snapshot["identity"]["group"] != "snow-ods-" + lane
            or registration["initial"]["kafka"]["bounds"][topic] != dict(partition=0, start=0, end=0)
            or not 1 <= len(snapshot["batches"]) <= MAX_BATCHES):
        raise ValueError("ODS is not the initialized single-partition real lane")
    batch_ids = [entry["batch_id"] for entry in snapshot["batches"]]
    if len(set(batch_ids)) != len(batch_ids) or set(ods_manifests) != set(batch_ids):
        raise ValueError("ODS manifest inventory differs from the immutable snapshot")
    previous, parent, entries = 0, None, []
    for entry in snapshot["batches"]:
        batch, manifest = entry["batch_id"], ods_manifests[entry["batch_id"]]
        exact(manifest, ("schema_version", "source", "identity", "starts", "ends", "parent", "files", "counts",
                         "original_min_accepted_at", "expires_at"), "ODS batch manifest")
        counts, files = manifest["counts"], manifest["files"]
        exact(counts, ("raw", "events", "changes", "quarantine", "tombstones"), "ODS counts")
        exact(files, FILES, "ODS files")
        end = manifest["ends"].get(key)
        if (not isinstance(batch, str) or not HEX.fullmatch(batch) or digest(canonical(manifest)) != batch
                or manifest["schema_version"] != 1 or manifest["source"] != "real"
                or manifest["identity"] != snapshot["identity"] or manifest["parent"] != parent
                or manifest["starts"] != {key: previous} or set(manifest["ends"]) != {key}
                or type(end) is not int or end <= previous
                or any(not isinstance(value, str) or not HEX.fullmatch(value) for value in files.values())
                or any(type(value) is not int or value < 0 for value in counts.values())
                or counts["raw"] != end - previous or counts["events"] != counts["raw"]
                or any(counts[name] for name in ("changes", "quarantine", "tombstones"))):
            raise ValueError("ODS manifest does not prove a continuous original Kafka metadata chain")
        lifetime(manifest["original_min_accepted_at"], manifest["expires_at"], lower, now)
        expected = dict(batch_id=batch, files=files, counts=counts,
                        original_min_accepted_at=manifest["original_min_accepted_at"], expires_at=manifest["expires_at"])
        if entry != expected:
            raise ValueError("ODS receipt differs from its batch manifest")
        entries.append(entry)
        previous = end
        prefix = dict(schema_version=2, source="real", identity=snapshot["identity"], offsets={key: end},
                      batches=list(entries), root=snapshot["root"], head_batch_id=batch)
        parent = digest(canonical(prefix))
    if snapshot["offsets"] != {key: previous} or ods_state["snapshot_id"] != parent or previous < through - after:
        raise ValueError("ODS head differs or cannot contain the selected source prefix")
    return dict(schema_version=1, kind="rebased_prefix_metadata", source="real", collector=identity,
                lane=lane, source_after=after, source_through=through, source_sequence_count=through - after,
                kafka_offsets=snapshot["offsets"], kafka_records=previous, input=ods_state["input"],
                original_min_accepted_at=registration["original_min_accepted_at"],
                epoch_expires_at=registration["expires_at"], observed_at=status["generated_at"],
                observation_scope="fixed_committed_accepted_prefix_after_explicit_gap", wall_clock_watermark=False,
                registration_sha256=digest(canonical(registration)), transport_sha256=digest(canonical(target)),
                rebase_sha256=digest(canonical(rebase)), source_status_sha256=digest(canonical(status)),
                sync_chain_sha256=digest(canonical(sync_chain)), ods_state_sha256=digest(canonical(ods_state)),
                payload_equivalence_verified=False, read_permission=False)
