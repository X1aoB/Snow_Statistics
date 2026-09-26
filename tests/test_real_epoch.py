import copy
import errno
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from snow_statistics import real_epoch
from snow_statistics.lifecycle import timestamp
from snow_statistics.publication import PublicationLockBusy, publication_lock
from snow_statistics.real_epoch import Epoch, compose_spec, labels, make_manifest, prepare

NOW = datetime.now(UTC)
IMAGE = "python@sha256:" + "a" * 64


class Docker:
    def __init__(self):
        self.c, self.v, self.actions = {}, {}, []
        self.fail_volume = False

    def containers(self):
        return set(self.c)

    def volumes(self):
        return set(self.v)

    def admit(self, manifest, stage, starting):
        return {"fixture": True}

    def inspect_container(self, name):
        return self.c[name]

    def inspect_volume(self, name):
        return self.v[name]

    def start(self, file, services):
        spec = json.loads(file.read_bytes())
        for role, value in spec["volumes"].items():
            self.v[value["name"]] = {"Labels": value["labels"], "Driver": "local", "Options": None}
        for role, value in spec["services"].items():
            if role not in services:
                continue
            mounts = [{"Type": "volume", "Name": spec["volumes"][part.split(":")[0]]["name"], "RW": True}
                      for part in value["volumes"] if part.split(":")[0] in spec["volumes"]]
            tmpfs = dict(item.split(":", 1) for item in value.get("tmpfs", []))
            mounts += [{"Type": "tmpfs", "Destination": path, "RW": True} for path in tmpfs]
            self.c[value["container_name"]] = dict(labels=value["labels"], running=True, restart=value["restart"],
                                                   mounts=mounts, entrypoint=value["entrypoint"],
                                                   deadline=value["environment"]["SNOW_EPOCH_EXPIRES_UNIX"], tmpfs=tmpfs)
        self.actions.append(("start", spec["name"]))

    def stop(self, name):
        self.actions.append(("stop", name))
        self.c[name]["running"] = False

    def remove_container(self, name):
        assert not self.c[name]["running"]
        self.actions.append(("container_rm", name))
        del self.c[name]

    def remove_volume(self, name):
        self.actions.append(("volume_rm", name))
        if not self.fail_volume:
            del self.v[name]


def candidate(tmp_path):
    manifest = make_manifest("fixture-a", NOW.isoformat(), {"PYTHON_IMAGE": IMAGE}, fixture=True, now=NOW)
    folder = tmp_path / "fixture-a"
    prepare(folder, manifest)
    docker = Docker()
    epoch = Epoch(folder, docker)
    epoch.start()
    return epoch, docker, manifest


def after(manifest):
    return timestamp(manifest["expires_at"]) + timedelta(seconds=1)


def test_physical_fixture_uses_only_owned_volumes_and_retirement_is_idempotent(tmp_path):
    epoch, docker, manifest = candidate(tmp_path)
    with pytest.raises(ValueError, match="not readable"):
        epoch.readable()
    unrelated = "snow-lab-synthetic-kafka"
    docker.v[unrelated] = {"Labels": {"source": "synthetic"}, "Driver": "local", "Options": None}
    result = epoch.retire(now=after(manifest), fixture_clock=True)
    assert docker.containers() == set() and docker.volumes() == {unrelated}
    assert len(result["volumes_removed"]) == 5
    assert result["readback_absent"]["volumes"] == sorted(manifest["volumes"].values())
    assert result["input_origin"] == "synthetic fixtures"
    assert result["production_online_database_touched"] is False
    assert result["forensic_media_erasure_claimed"] is False
    assert epoch.retire(now=after(manifest), fixture_clock=True) == result
    with pytest.raises(ValueError, match="restart"):
        epoch.start()


@pytest.mark.parametrize("wrong", ["labels", "restart", "guard", "anonymous_volume", "external_volume"])
def test_any_unknown_storage_or_ownership_prevents_all_mutations(tmp_path, wrong):
    epoch, docker, manifest = candidate(tmp_path)
    name = manifest["containers"]["fixture"]
    first = manifest["volumes"]["kafka"]
    if wrong == "labels":
        docker.v[first]["Labels"] = labels(manifest) | {"org.snow-statistics.generation": "foreign"}
    if wrong == "restart":
        docker.c[name]["restart"] = "always"
    if wrong == "guard":
        docker.c[name]["deadline"] = "9999999999"
    if wrong == "anonymous_volume":
        docker.c[name]["mounts"].append({"Type": "volume", "Name": "unknown", "RW": True})
    if wrong == "external_volume":
        docker.v[first]["Options"] = {"device": "/business"}
    before = list(docker.actions)
    with pytest.raises(ValueError):
        epoch.retire(now=after(manifest), fixture_clock=True)
    assert docker.actions == before


def test_expired_resource_that_remains_blocks_restart_then_retry_verifies_absence(tmp_path):
    epoch, docker, manifest = candidate(tmp_path)
    docker.fail_volume = True
    with pytest.raises(RuntimeError, match="volume remains"):
        epoch.retire(now=after(manifest), fixture_clock=True)
    assert not json.loads((epoch.directory / "gate.json").read_bytes())["open"]
    with pytest.raises(ValueError, match="failed physical"):
        epoch.start()
    docker.fail_volume = False
    result = epoch.retire(now=after(manifest), fixture_clock=True)
    assert not docker.volumes() and not docker.containers()
    assert len(result["readback_absent"]["volumes"]) == 5


def test_stopping_preserves_data_and_unexpired_epochs_cannot_be_deleted(tmp_path):
    epoch, docker, manifest = candidate(tmp_path)
    with pytest.raises(ValueError, match="unexpired"):
        epoch.retire()
    result = epoch.stop()
    assert result["data_deleted"] is False and len(docker.volumes()) == 5
    assert not next(iter(docker.c.values()))["running"]
    with pytest.raises(ValueError, match="real wall clock"):
        epoch.retire(now=after(manifest))


def test_unknown_mount_blocks_deletion_but_never_prevents_stopping_owned_readers(tmp_path):
    epoch, docker, manifest = candidate(tmp_path)
    name = manifest["containers"]["fixture"]
    docker.c[name]["mounts"].append({"Type": "volume", "Name": "unregistered", "RW": True})
    with pytest.raises(ValueError, match="Unknown writable"):
        epoch.retire(now=after(manifest), fixture_clock=True)
    assert docker.c[name]["running"] is True
    assert epoch.stop()["data_deleted"] is False
    assert docker.c[name]["running"] is False and len(docker.v) == 5


def test_changed_frozen_config_blocks_start_and_engine_clock_cannot_be_faked(tmp_path):
    epoch, docker, manifest = candidate(tmp_path)
    epoch.stop()
    (epoch.directory / "epoch-guard.sh").write_text("exit 0")
    with pytest.raises(ValueError, match="configuration changed"):
        epoch.start()
    manifest["mode"] = "engines"
    manifest["input_origin"] = "real"
    manifest["containers"] = {role: manifest["project"] + "-" + role for role in ("kafka", "doris-fe", "doris-be", "jobmanager", "taskmanager")}
    (epoch.directory / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="Only synthetic"):
        epoch.retire(now=after(manifest), fixture_clock=True)


def test_full_engine_profile_has_finite_independent_storage_and_pinned_jar(tmp_path):
    images = {key: IMAGE for key in ("KAFKA_IMAGE", "DORIS_FE_IMAGE", "DORIS_BE_IMAGE", "FLINK_IMAGE")}
    manifest = make_manifest("epoch-a", NOW.isoformat(), images, now=NOW)
    jar = tmp_path / "fixture.jar"
    jar.write_bytes(b"synthetic jar fixture, not runnable")
    spec = compose_spec(manifest, tmp_path / "epoch-a", "192.168.65.3", jar)
    assert len(spec["services"]) == 5 and len(spec["volumes"]) == 5
    for item in spec["services"].values():
        assert item["restart"] == "no"
        assert item["labels"]["org.snow-statistics.source"] == "real"
        assert item["entrypoint"] == ["sh", "/snow/epoch-guard.sh"]
    assert set(value["name"] for value in spec["volumes"].values()) == set(manifest["volumes"].values())
    for role in ("jobmanager", "taskmanager"):
        env = spec["services"][role]["environment"]
        assert env["SNOW_REAL_EPOCH_ID"] == manifest["epoch_id"]
        assert env["SNOW_REAL_EPOCH_GENERATION"] == manifest["generation"]
        assert env["SNOW_REAL_EPOCH_FROM"] == manifest["original_min_accepted_at"]
        assert env["SNOW_REAL_EPOCH_UNTIL"] == manifest["expires_at"]
    for role in ("doris-fe", "doris-be"):
        assert '"$$SNOW_DORIS_IP"' in spec["services"][role]["command"][2]
    corrupt = copy.deepcopy(manifest)
    corrupt["volumes"]["kafka"] = "snow-lab-control_kafka"
    with pytest.raises(ValueError, match="escaped"):
        compose_spec(corrupt, tmp_path, "192.168.65.3", jar)


def test_real_storage_stage_precedes_realtime_and_does_not_claim_engine_acceptance(tmp_path):
    images = {key: IMAGE for key in ("KAFKA_IMAGE", "DORIS_FE_IMAGE", "DORIS_BE_IMAGE", "FLINK_IMAGE")}
    manifest = make_manifest("epoch-stage", NOW.isoformat(), images, now=NOW)
    jar = tmp_path / "fixture.jar"
    jar.write_bytes(b"non-runnable synthetic jar")
    directory = tmp_path / "epoch-stage"
    prepare(directory, manifest, "192.168.65.3", jar)
    docker = Docker()
    epoch = Epoch(directory, docker)
    with pytest.raises(ValueError, match="storage stage"):
        epoch.start("realtime")
    storage = epoch.start("storage")
    assert storage["engines_verified"] is False and len(docker.c) == 3
    assert json.loads((directory / "gate.json").read_bytes())["open"] is False
    realtime = epoch.start("realtime")
    assert realtime["engines_verified"] is False and len(docker.c) == 5
    assert json.loads((directory / "gate.json").read_bytes())["open"] is True
    assert epoch.readable()["event_lane"] == "epoch_stage"
    docker.c[manifest["containers"]["taskmanager"]]["running"] = False
    with pytest.raises(ValueError, match="missing"):
        epoch.readable()


def test_all_synthetic_engine_containers_running_cannot_grant_production_read_permission(tmp_path):
    images = {key: IMAGE for key in ("KAFKA_IMAGE", "DORIS_FE_IMAGE", "DORIS_BE_IMAGE", "FLINK_IMAGE")}
    manifest = make_manifest("fixture-engine-01", NOW.isoformat(), images, synthetic_engine_test=True, now=NOW)
    jar = tmp_path / "fixture.jar"
    jar.write_bytes(b"non-runnable synthetic jar")
    folder = tmp_path / manifest["epoch_id"]
    prepare(folder, manifest, "192.168.65.3", jar)
    epoch = Epoch(folder, Docker())
    epoch.start("storage")
    epoch.start("realtime")
    gate = json.loads((folder / "gate.json").read_bytes())
    assert gate["fixture_ready"] is True and gate["open"] is False and gate["serves_real_data"] is False
    with pytest.raises(ValueError, match="not readable"):
        epoch.readable()


class RetryClock:
    def __init__(self):
        self.elapsed, self.sleeps = 0, []

    def monotonic(self):
        return self.elapsed

    def sleep(self, delay):
        self.sleeps.append(delay)
        self.elapsed += delay

    def time(self):
        return NOW.timestamp() + self.elapsed


def supervisor_clock(monkeypatch):
    clock = RetryClock()
    monkeypatch.setattr(real_epoch, "time", clock)
    monkeypatch.setattr(real_epoch, "signal", SimpleNamespace(signal=lambda *_: None, SIGTERM=15, SIGINT=2))
    return clock


def test_supervisor_recovers_transient_lock_contention_before_resuming_watch(tmp_path, monkeypatch):
    _, docker, _ = candidate(tmp_path)
    clock = supervisor_clock(monkeypatch)
    attempts = []

    def expire(*_):
        attempts.append(clock.elapsed)
        assert all(item["running"] for item in docker.c.values())
        if len(attempts) <= 2:
            raise PublicationLockBusy(errno.EAGAIN, "synthetic held lock")
        return [{"retired": True}]

    def sleep(delay):
        if delay > 1:
            raise SystemExit(0)  # Stop the otherwise perpetual watch after its first successful cycle.
        clock.sleep(delay)

    monkeypatch.setattr(real_epoch, "expire_due", expire)
    monkeypatch.setattr(real_epoch, "time", SimpleNamespace(monotonic=clock.monotonic, time=clock.time, sleep=sleep))
    with pytest.raises(SystemExit):
        real_epoch.supervise(tmp_path, docker)
    assert attempts == [0, 1, 2]
    assert all(not item["running"] for item in docker.c.values())
    assert len(docker.v) == 5  # Normal shutdown only stops readers; no data is deleted.


def test_continuous_lock_contention_exhausts_budget_and_stops_owned_readers(tmp_path, monkeypatch):
    _, docker, _ = candidate(tmp_path)
    clock = supervisor_clock(monkeypatch)
    attempts = []
    failure = PublicationLockBusy(errno.EAGAIN, "synthetic persistent held lock")

    def expire(*_):
        attempts.append(clock.elapsed)
        raise failure

    monkeypatch.setattr(real_epoch, "expire_due", expire)
    with pytest.raises(PublicationLockBusy) as caught:
        real_epoch.supervise(tmp_path, docker)
    assert caught.value is failure
    assert clock.elapsed == 60 and len(attempts) == 61 and max(clock.sleeps) == 1
    assert all(not item["running"] for item in docker.c.values())
    assert len(docker.v) == 5


def test_cleanup_body_blocking_io_error_is_not_retried_and_stops_owned_readers(tmp_path, monkeypatch):
    _, docker, _ = candidate(tmp_path)
    clock = supervisor_clock(monkeypatch)
    failure = BlockingIOError(errno.EAGAIN, "synthetic cleanup body I/O failure")
    attempts = []

    def expire(*_):
        attempts.append(clock.elapsed)
        with publication_lock(tmp_path / "fixture-a"):
            raise failure

    monkeypatch.setattr(real_epoch, "expire_due", expire)
    with pytest.raises(BlockingIOError) as caught:
        real_epoch.supervise(tmp_path, docker)
    assert caught.value is failure and not isinstance(caught.value, PublicationLockBusy)
    assert attempts == [0] and clock.sleeps == []
    assert all(not item["running"] for item in docker.c.values())
    assert len(docker.v) == 5


def test_successful_cleanup_resets_retry_budget_and_returns_actual_receipt(tmp_path, monkeypatch):
    clock = supervisor_clock(monkeypatch)
    attempts, receipt = [], {"retired": True, "source": "synthetic fixture"}

    def expire(*_):
        attempts.append(clock.elapsed)
        if len(attempts) % 60:
            raise PublicationLockBusy(errno.EAGAIN, "synthetic held lock")
        return [receipt]

    monkeypatch.setattr(real_epoch, "expire_due", expire)
    for _ in range(2):
        result = real_epoch._expire_due_with_lock_retry(tmp_path, Docker())
        assert result[0] is receipt
    assert len(attempts) == 120 and clock.elapsed == 118
