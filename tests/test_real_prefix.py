"""Synthetic fixtures produced by the actual sync/landing metadata writers."""
import copy
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from test_landing import Broker, Sink, record
from test_real_ods import COLLECTOR

from snow_statistics.io import digest, write_json
from snow_statistics.landing import acknowledge, capture, land
from snow_statistics.publication import canonical
from snow_statistics.real_prefix import validate_rebased_prefix
from snow_statistics.simulator import generate
from snow_statistics.source_cursor import rebase
from snow_statistics.sync import sync_once


def actual_metadata(tmp_path, *, duplicate=False, truncated=False):
    config = json.loads((Path(__file__).resolve().parents[1] / "deploy/real-run.example.json").read_bytes())
    config["lane"] = lane = "fixture-prefix"
    event_lane = lane.replace("-", "_")
    source = copy.deepcopy(COLLECTOR)
    now = datetime.now(UTC) + timedelta(seconds=2)
    origin = now - timedelta(hours=1)
    status = source | dict(expired_through=201, earliest_available_seq=202, latest_accepted_seq=205,
                           aggregate_cursor=205, aggregate_gap=False, generated_at=now.isoformat())
    target = dict(schema_version=1, url=config["collector_url"],
                  bootstrap=config["nodes"]["snow-analysis"] + ":9092", source="real", lane=event_lane)
    old, sync, ods = (tmp_path / name for name in ("old", "sync", "ods"))
    write_json(old / "target.json", target | {"lane": "fixture_old"})
    rebased = rebase(old, sync, status, after=201, reason="synthetic_retention_gap", identity=target)
    events = generate(users=1)["events"][:4]
    for index, row in enumerate(events):
        row.update(seq=202 + index, source="real", accepted_at=(origin + timedelta(seconds=index)).isoformat())
        # Acceptance today deliberately does not imply today's business date.
        row["event"]["occurred_at"] = (origin - timedelta(days=2)).isoformat()
    published = []
    for offset in (0, 2):
        rows = events[offset:offset + 2]
        sync_once(sync, lambda after: dict(schema_version=1, events=rows, next_cursor=rows[-1]["seq"]),
                  published.append, identity=target, status=lambda: status, now=now)
    broker, sink = Broker(), Sink()
    topic = "snow.real." + event_lane + ".events.v1"
    broker.id = dict(cluster_id="synthetic-cluster", topic_ids={topic: "synthetic-topic-id"},
                     group="snow-ods-" + lane, collector=source)
    kafka_rows = published + published[:1] if duplicate else published[:3] if truncated else published
    broker.rows = {topic + ":0": [record(row, index, topic) for index, row in enumerate(kafka_rows)]}
    sink.root = "hdfs://" + config["nodes"]["snow-control"] + ":9000/snow/ods/real/kafka/" + lane
    while capture(ods, broker, max_records=2, provenance="real", event_lane=event_lane, now=now):
        land(ods, sink, now=now)
        acknowledge(ods, broker)
    artifacts = json.loads((sync / "data/registry.json").read_bytes())["artifacts"]
    names = [name for name in artifacts if not name.endswith(".manifest.json")]
    manifests = {name: json.loads((sync / "data" / (name + ".manifest.json")).read_bytes()) for name in names}
    checksums = {name: digest((sync / "data" / name).read_bytes()) for name in names}
    state = json.loads((ods / "state.json").read_bytes())
    initial = dict(identity=dict(cluster_id=broker.id["cluster_id"], topic_ids=broker.id["topic_ids"]),
                   bounds={topic: dict(partition=0, start=0, end=0)})
    registration = dict(epoch_id=lane, collector=source, original_min_accepted_at=origin.isoformat(),
                         expires_at=(origin + timedelta(days=7)).isoformat(), initial={"kafka": initial})
    return dict(config=config, registration=registration, source=source, target=target,
                cursor=json.loads((sync / "cursor.json").read_bytes()), rebase=rebased, status=status,
                sync_manifests=manifests, lifecycle_artifacts=artifacts, archive_checksums=checksums,
                ods_state=state, ods_landed=json.loads((ods / "landed.json").read_bytes()),
                ods_manifests={entry["batch_id"]: json.loads((ods / "batches" / entry["batch_id"] / "manifest.json").read_bytes())
                               for entry in state["batches"]}, now=now)


def test_nonzero_rebase_source_prefix_is_distinct_from_kafka_offsets(tmp_path):
    inputs = actual_metadata(tmp_path)
    result = validate_rebased_prefix(**inputs)
    assert (result["source_after"], result["source_through"], result["source_sequence_count"]) == (201, 205, 4)
    assert result["kafka_records"] == 4 and list(result["kafka_offsets"].values()) == [4]
    assert result["input"] == inputs["ods_state"]["input"]
    assert result["registration_sha256"] == digest(canonical(inputs["registration"]))
    assert result["epoch_expires_at"] == inputs["registration"]["expires_at"]
    assert result["wall_clock_watermark"] is False
    assert result["payload_equivalence_verified"] is False and result["read_permission"] is False
    assert not {"date", "date_from", "date_to", "closed_day", "complete", "passed"} & set(result)


def test_kafka_replay_does_not_make_metadata_claim_payload_equality(tmp_path):
    result = validate_rebased_prefix(**actual_metadata(tmp_path, duplicate=True))
    assert result["source_sequence_count"] == 4 and result["kafka_records"] == 5
    assert result["payload_equivalence_verified"] is False


def test_incomplete_kafka_prefix_cannot_cover_all_selected_source_sequences(tmp_path):
    with pytest.raises(ValueError, match="cannot contain"):
        validate_rebased_prefix(**actual_metadata(tmp_path, truncated=True))


@pytest.mark.parametrize("change", ["old_zero", "behind", "ahead", "gap", "wrong_generation", "wrong_lane"])
def test_rebased_source_boundary_and_identity_cannot_drift(tmp_path, change):
    inputs = actual_metadata(tmp_path)
    if change == "old_zero":
        inputs["rebase"]["after"] = 0
    elif change == "behind":
        inputs["cursor"]["cursor"] = 204
    elif change == "ahead":
        inputs["cursor"]["cursor"] = 206
    elif change == "gap":
        inputs["status"]["aggregate_gap"] = True
    elif change == "wrong_generation":
        inputs["status"]["generation"] = "00000000-0000-0000-0000-000000000001"
    else:
        inputs["target"]["lane"] = "another_lane"
    with pytest.raises(ValueError):
        validate_rebased_prefix(**inputs)


@pytest.mark.parametrize("change", ["hole", "overlap", "hash", "missing_companion", "expired", "extended", "copied"])
def test_sync_archive_chain_and_original_lifetime_are_required(tmp_path, change):
    inputs = actual_metadata(tmp_path)
    name = sorted(inputs["sync_manifests"])[-1]
    item = inputs["lifecycle_artifacts"][name]
    if change in {"hole", "overlap"}:
        inputs["sync_manifests"][name]["after"] += 1 if change == "hole" else -1
    elif change == "hash":
        inputs["archive_checksums"][name] = "0" * 64
    elif change == "missing_companion":
        del inputs["lifecycle_artifacts"][name + ".manifest.json"]
    elif change == "expired":
        inputs["now"] += timedelta(days=7)
    else:
        if change == "extended":
            item["expires_at"] = (inputs["now"] + timedelta(days=8)).isoformat()
        else:
            item["copied_from"] = "unregistered-copy"
        inputs["lifecycle_artifacts"][name + ".manifest.json"] = copy.deepcopy(item)
    with pytest.raises(ValueError):
        validate_rebased_prefix(**inputs)


@pytest.mark.parametrize("change", ["landed", "manifest", "parent", "inventory", "initial_offset"])
def test_ods_metadata_chain_and_initial_kafka_identity_are_required(tmp_path, change):
    inputs = actual_metadata(tmp_path)
    batch = inputs["ods_state"]["batches"][-1]["batch_id"]
    if change == "landed":
        inputs["ods_landed"]["snapshot_id"] = "0" * 64
    elif change == "manifest":
        inputs["ods_manifests"][batch]["counts"]["raw"] += 1
    elif change == "parent":
        inputs["ods_manifests"][batch]["parent"] = "0" * 64
    elif change == "inventory":
        del inputs["ods_manifests"][batch]
    else:
        bounds = inputs["registration"]["initial"]["kafka"]["bounds"]
        next(iter(bounds.values()))["start"] = 201
    with pytest.raises(ValueError):
        validate_rebased_prefix(**inputs)


def test_validation_is_metadata_only_and_does_not_mutate_inputs(tmp_path):
    inputs = actual_metadata(tmp_path)
    before = copy.deepcopy(inputs)
    validate_rebased_prefix(**inputs)
    assert inputs == before
