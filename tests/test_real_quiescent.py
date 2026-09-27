"""Synthetic API responses only; none of these tests is engine evidence."""
import base64
import copy
import json
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from test_real_epoch import Docker
from test_real_remote_lifecycle import FixtureHdfs, FixtureOds

from snow_statistics.io import digest, write_json
from snow_statistics.publication import canonical
from snow_statistics.real_epoch import Epoch, make_manifest, prepare
from snow_statistics.real_quiescent import (
    STATES,
    TABLES,
    StoppedStorage,
    WindowGuard,
    WriterRegistry,
    backend_resources,
    bind_ods,
    expected_parameters,
    storage,
    topic_names,
    validate_doris_write,
    validate_stopped_receipt,
)
from snow_statistics.real_remote_lifecycle import RealRemoteLifecycle
from snow_statistics.simulator import generate
from snow_statistics.sync import sync_once

COLLECTOR = dict(schema_version=1, source="real", instance_id="00000000-0000-0000-0000-000000000001",
                 generation="00000000-0000-0000-0000-000000000002")
NOW = datetime.now(UTC)


class FixtureDocker(Docker):
    def start(self, file, services):
        old = set(self.c)
        super().start(file, services)
        spec = json.loads(file.read_bytes())
        for role in services:
            data = spec["services"][role]
            value = self.c[data["container_name"]]
            value["object_id"] = digest(data["container_name"].encode())
            value["created_at"] = NOW.isoformat()
            value["epoch_environment"] = {key: value for key, value in data["environment"].items() if key.startswith("SNOW_REAL_EPOCH_")}
            for mount in data["volumes"]:
                if mount.split(":", 1)[0] not in spec["volumes"]:
                    source, target, _ = mount.rsplit(":", 2)
                    value["mounts"].append(dict(Type="bind", Source=source, Destination=target, RW=False))
        for name in self.v:
            self.v[name]["CreatedAt"] = NOW.isoformat()
        assert old <= set(self.c)


class FixtureProbe:
    """Test double whose API matches the required real in-process probe."""
    def __init__(self, manifest):
        names = topic_names(manifest)
        cluster = base64.urlsafe_b64encode(UUID(manifest["generation"]).bytes).decode().rstrip("=")
        self.initial = dict(kafka=dict(identity=dict(cluster_id=cluster, topic_ids={t: str(i) * 22 for i, t in enumerate(names, 1)}),
                                      bounds={t: dict(partition=0, start=0, end=0) for t in names}),
                            doris=dict(database="snow_real_" + manifest["event_lane"], tables={name: 0 for name in TABLES}),
                            flink={"jobs": []}, state={name: [] for name in STATES})
        self.job = dict(job_id="a" * 32, state="RUNNING", jar_sha256=manifest["jar"]["sha256"],
                        parameters=expected_parameters(manifest, self.initial["kafka"]))
        self.reads = []

    def read_initial_state(self, manifest, collector):
        self.reads.append(("initialize", collector))
        return copy.deepcopy(self.initial)

    def read_job(self, job_id):
        self.reads.append(("job", job_id))
        return copy.deepcopy(self.job)


def candidate(tmp_path, *, now=NOW, registered=True):
    jar = tmp_path / "fixture.jar"
    jar.write_bytes(b"synthetic jar bytes, not an executable engine")
    images = {k: "fixture@sha256:" + "a" * 64 for k in ("KAFKA_IMAGE", "DORIS_FE_IMAGE", "DORIS_BE_IMAGE", "FLINK_IMAGE")}
    manifest = make_manifest("candidate-one", (now - timedelta(hours=1)).isoformat(), images, now=now)
    directory = tmp_path / "epochs" / manifest["epoch_id"]
    prepare(directory, manifest, "192.168.100.103", jar)
    docker = FixtureDocker()
    epoch = Epoch(directory, docker)
    # No real subprocess/network API is invoked by this fixture.
    docker.start(directory / "compose.json", list(manifest["containers"]))
    registry = WriterRegistry(epoch)
    probe = FixtureProbe(manifest)
    if registered:
        registry.initialize(COLLECTOR, probe, now=now)
        registry.record_job("a" * 32, probe, now=now)
    return registry, docker, probe


def snapshot(registry):
    value = registry.read()
    original = value["original_min_accepted_at"]
    topic = next(t for t in value["initial"]["kafka"]["identity"]["topic_ids"] if t.endswith(".events.v1"))
    return dict(schema_version=2, source="real", identity=dict(collector=COLLECTOR, cluster_id=value["initial"]["kafka"]["identity"]["cluster_id"],
                topic_ids={topic: value["initial"]["kafka"]["identity"]["topic_ids"][topic]}),
                offsets={topic + ":0": 1}, root="hdfs://snow-control:9000/snow/ods/real/kafka/candidate-one", head_batch_id="a" * 64,
                batches=[dict(batch_id="a" * 64, files={}, counts=dict(events=1, quarantine=0), original_min_accepted_at=original,
                              expires_at=(datetime.fromisoformat(original) + timedelta(days=7)).isoformat())])


def test_immutable_initialization_and_actual_job_readback_are_both_required(tmp_path):
    registry, _, probe = candidate(tmp_path, registered=False)
    with pytest.raises(ValueError, match="has not been registered"):
        registry.ready()
    first = registry.initialize(COLLECTOR, probe)
    original = registry.path.read_bytes()
    assert probe.reads == [("initialize", COLLECTOR)]
    with pytest.raises(ValueError, match="Flink writer"):
        registry.ready()
    with pytest.raises(ValueError, match="immutable"):
        registry.initialize(COLLECTOR, probe)
    probe.job["parameters"]["readable_from"] = (NOW - timedelta(days=2)).isoformat()
    with pytest.raises(ValueError, match="frozen epoch"):
        registry.record_job("a" * 32, probe)
    assert not registry.job_path.exists()
    probe.job["parameters"] = expected_parameters(registry.epoch.read(), first["initial"]["kafka"])
    registry.record_job("a" * 32, probe)
    assert registry.ready()["collector"] == COLLECTOR
    assert registry.path.read_bytes() == original


@pytest.mark.parametrize("wrong", ["topic_rows", "extra_table", "doris_rows", "job", "checkpoint", "fixture", "missing_volume"])
def test_existing_payload_partial_resources_or_fixture_cannot_initialize(tmp_path, wrong):
    registry, docker, probe = candidate(tmp_path, registered=False)
    if wrong == "topic_rows":
        next(iter(probe.initial["kafka"]["bounds"].values()))["end"] = 1
    elif wrong == "extra_table":
        probe.initial["doris"]["tables"]["private_other_table"] = 0
    elif wrong == "doris_rows":
        probe.initial["doris"]["tables"]["events_realtime"] = 1
    elif wrong == "job":
        probe.initial["flink"]["jobs"] = ["running"]
    elif wrong == "checkpoint":
        probe.initial["state"]["/checkpoints"] = ["expired/private_payload"]
    elif wrong == "fixture":
        value = registry.epoch.read()
        value.update(mode="synthetic_engine_test", input_origin="synthetic fixtures")
        write_json(registry.epoch.directory / "manifest.json", value)
    else:
        del docker.v[next(iter(docker.v))]
    with pytest.raises(ValueError):
        registry.initialize(COLLECTOR, probe)
    assert not registry.path.exists()


def test_stopped_receipt_explicitly_proves_only_unexpired_physical_storage(tmp_path):
    registry, docker, _ = candidate(tmp_path)
    snap = snapshot(registry)
    with pytest.raises(ValueError, match="stopped"):
        StoppedStorage(registry, snap).check()
    registry.epoch.stop()
    now = datetime.now(UTC)
    stopped = StoppedStorage(registry, snap)
    evidence = stopped.check(now=now)
    assert evidence["verification"] == "stopped_storage_unexpired"
    assert evidence["sql_cleanup_verified"] is False and evidence["kafka_records_scanned"] is False
    assert len(docker.volumes()) == 5 and not any(v["running"] for v in docker.c.values())
    for name, adapter in stopped.adapters().items():
        resources = backend_resources(registry.read())[name]
        receipt = adapter.verify_stopped(resources, now)
        validate_stopped_receipt(receipt, name, resources, COLLECTOR, snap, now)
        assert "live_records" not in receipt and "remaining_expired" not in receipt
        forged = copy.deepcopy(receipt)
        forged["evidence"]["sql_cleanup_verified"] = True
        forged["evidence_sha256"] = digest(canonical(forged["evidence"]))
        with pytest.raises(ValueError):
            validate_stopped_receipt(forged, name, resources, COLLECTOR, snap, now)


@pytest.mark.parametrize("wrong", ["recreated_volume", "recreated_container", "volume_generation", "foreign_consumer", "extra_owned", "changed_mount", "missing_container", "lost_job", "changed_code", "flink_window"])
def test_stopped_storage_cannot_adopt_missing_replaced_or_unowned_copies(tmp_path, wrong):
    registry, docker, _ = candidate(tmp_path)
    snap = snapshot(registry)
    registry.epoch.stop()
    volume = next(iter(docker.v))
    container = next(iter(docker.c))
    if wrong == "recreated_volume":
        docker.v[volume]["CreatedAt"] = (NOW + timedelta(seconds=1)).isoformat()
    elif wrong == "recreated_container":
        docker.c[container]["object_id"] = "f" * 64
    elif wrong == "volume_generation":
        docker.v[volume]["Labels"]["org.snow-statistics.generation"] = "foreign"
    elif wrong == "foreign_consumer":
        docker.c["foreign"] = dict(labels={}, running=False, mounts=[dict(Type="volume", Name=volume)])
    elif wrong == "extra_owned":
        docker.v["extra"] = copy.deepcopy(docker.v[volume])
    elif wrong == "changed_mount":
        mount = next(m for m in docker.c[container]["mounts"] if m["Type"] == "bind")
        mount["Source"] = "/wrong/frozen-jar"
    elif wrong == "missing_container":
        del docker.c[container]
    elif wrong == "lost_job":
        registry.job_path.unlink()
    elif wrong == "flink_window":
        name = registry.epoch.read()["containers"]["jobmanager"]
        docker.c[name]["epoch_environment"]["SNOW_REAL_EPOCH_GENERATION"] = "00000000-0000-0000-0000-000000000010"
    else:
        value = json.loads(registry.path.read_bytes())
        value["writers"][next(iter(value["writers"]))] = "f" * 64
        write_json(registry.path, value)
    with pytest.raises(ValueError):
        StoppedStorage(registry, snap).check()
    assert docker.volumes()  # Rejection does not prune unexpired resources.


def test_collector_kafka_generation_and_original_ods_window_must_all_match(tmp_path):
    registry, _, _ = candidate(tmp_path)
    value = registry.read()
    snap = snapshot(registry)
    bind_ods(value, snap)
    for key in ("collector", "cluster_id", "topic_ids"):
        changed = copy.deepcopy(snap)
        changed["identity"][key] = {} if key != "cluster_id" else "foreign"
        with pytest.raises(ValueError, match="generation"):
            bind_ods(value, changed)
    changed = copy.deepcopy(snap)
    changed["batches"][0]["original_min_accepted_at"] = (NOW - timedelta(days=1)).isoformat()
    with pytest.raises(ValueError, match="predates"):
        bind_ods(value, changed)


def test_expired_physical_epoch_is_actually_deleted_before_admission_is_refused(tmp_path):
    old = NOW - timedelta(days=8)
    registry, docker, _ = candidate(tmp_path, now=old)
    with pytest.raises(ValueError, match="expired"):
        StoppedStorage(registry, {}).check()
    assert not docker.containers() and not docker.volumes()
    receipt = json.loads((registry.epoch.directory / "retirement.json").read_bytes())
    assert receipt["readback_absent"]["volumes"] and not receipt["forensic_media_erasure_claimed"]


def test_failed_physical_delete_remains_closed_and_does_not_issue_a_receipt(tmp_path):
    registry, docker, _ = candidate(tmp_path, now=NOW - timedelta(days=8))
    docker.fail_volume = True
    with pytest.raises(RuntimeError, match="volume remains"):
        StoppedStorage(registry, {}).check()
    assert not (registry.epoch.directory / "retirement.json").exists()
    assert not json.loads((registry.epoch.directory / "gate.json").read_bytes())["open"]
    assert docker.volumes()


def test_window_guard_rejects_old_future_changed_source_and_deadline(tmp_path):
    registry, _, _ = candidate(tmp_path)
    source_file = tmp_path / "source.json"
    write_json(source_file, COLLECTOR)
    guard = WindowGuard(registry, source_file, clock=lambda: NOW)
    guard(dict(source="real", accepted_at=NOW.isoformat()))
    for accepted in (NOW - timedelta(hours=2), NOW + timedelta(seconds=1)):
        with pytest.raises(ValueError, match="outside"):
            guard(dict(source="real", accepted_at=accepted.isoformat()))
    write_json(source_file, COLLECTOR | {"generation": "00000000-0000-0000-0000-000000000003"})
    with pytest.raises(ValueError, match="different collector"):
        guard(dict(source="real", accepted_at=NOW.isoformat()))


def test_guard_runs_before_new_archive_and_replay_does_not_advance_or_renew(tmp_path):
    row = generate(users=1)["events"][0]  # Synthetic event; no real user data.
    response = dict(events=[row], next_cursor=1)
    def refuse(_):
        raise ValueError("epoch window")
    with pytest.raises(ValueError, match="epoch window"):
        sync_once(tmp_path, lambda _: response, lambda _: pytest.fail("send"), before_publish=refuse)
    assert not (tmp_path / "pending.json").exists() and not (tmp_path / "batches").exists()
    with pytest.raises(RuntimeError):
        sync_once(tmp_path, lambda _: response, lambda _: (_ for _ in ()).throw(RuntimeError("ACK failed")))
    files = {p.relative_to(tmp_path): p.read_bytes() for p in tmp_path.rglob("*.json")}
    with pytest.raises(ValueError, match="epoch window"):
        sync_once(tmp_path, lambda _: pytest.fail("must replay"), lambda _: pytest.fail("send"), before_publish=refuse)
    assert {p.relative_to(tmp_path): p.read_bytes() for p in tmp_path.rglob("*.json")} == files
    assert not (tmp_path / "cursor.json").exists()


def test_actual_stopped_adapter_integrates_with_hdfs_cleanup_and_missing_copy_blocks(tmp_path):
    registry, _, _ = candidate(tmp_path)
    snap = snapshot(registry)
    registry.epoch.stop()
    now = datetime.now(UTC)
    ods = tmp_path / "ods"
    ods.mkdir()
    sink = FixtureOds(ods)
    sink.root = snap["root"]
    sink.body = canonical(snap)
    token = digest(sink.body)
    sink.input = sink.root + "/snapshots/" + token + "/_snapshot.json"
    write_json(ods / "state.json", snap | dict(snapshot_id=token, batch_id=snap["head_batch_id"], input=sink.input))
    manager = RealRemoteLifecycle(tmp_path / "metadata")
    manager.initialize("hdfs://snow-control:9000/snow/warehouse/real/candidate-one",
                       "hdfs://snow-control:9000/snow/auxiliary/real/candidate-one", sink.root,
                       COLLECTOR["instance_id"], COLLECTOR["generation"])
    resources = backend_resources(registry.read())
    for name, entries in resources.items():
        for resource, entry in entries.items():
            manager.register_backend(name, resource, entry["kind"], entry["original_min_accepted_at"])
    adapters = StoppedStorage(registry, snap).adapters()
    result = manager.cleanup(FixtureHdfs(), ods, sink, backend_checks=adapters, now=now)
    assert result["backends"]["doris"]["verification"] == "stopped_storage_unexpired"
    assert result["next_expiry"] == registry.read()["expires_at"]
    value = json.loads(manager.registry.read_bytes())
    value["backends"]["checkpoint"] = {"state": "not_initialized", "resources": {}}
    write_json(manager.registry, value)
    with pytest.raises(ValueError, match="Every initialized"):
        manager.cleanup(FixtureHdfs(), ods, sink, backend_checks=adapters, now=now)
    assert manager.journal.exists()


def test_failed_partial_storage_does_not_masquerade_as_an_empty_backend(tmp_path):
    registry, docker, _ = candidate(tmp_path)
    registry.epoch.stop()
    del docker.v[next(iter(docker.v))]
    with pytest.raises(ValueError, match="Every registered"):
        storage(registry.epoch, running=False)


def test_doris_aggregate_age_is_distinct_from_original_event_acceptance(tmp_path):
    from test_real_publication import packages
    registry, _, _ = candidate(tmp_path)
    package, _ = packages()
    manifest = registry.epoch.read()
    day = (NOW - timedelta(days=2)).date().isoformat()
    package["manifest"].update(date_from=day, date_to=day, cutoff=NOW.isoformat(),
        input="hdfs://snow-control:9000/snow/ods/real/kafka/" + manifest["epoch_id"] + "/snapshots/" + "a" * 64 + "/_snapshot.json")
    package["manifest"]["input_snapshot"].update(collector=COLLECTOR, offsets={topic_names(manifest)[0] + ":0": 2})
    package["daily"][0]["date"] = day
    assert validate_doris_write(package, registry, COLLECTOR)["epoch_id"] == manifest["epoch_id"]
    package["manifest"]["input_snapshot"]["collector"] = COLLECTOR | {"generation": "00000000-0000-0000-0000-000000000003"}
    with pytest.raises(ValueError, match="collector"):
        validate_doris_write(package, registry, COLLECTOR)
    package["manifest"]["input_snapshot"]["collector"] = COLLECTOR
    old = (NOW - timedelta(days=87)).date().isoformat()
    package["manifest"].update(date_from=old, date_to=old)
    package["daily"][0]["date"] = old
    with pytest.raises(ValueError, match="aggregate expires"):
        validate_doris_write(package, registry, COLLECTOR)
