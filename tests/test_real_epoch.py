import copy
import json
from datetime import UTC, datetime, timedelta

import pytest

from snow_statistics.lifecycle import timestamp
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
            self.c[value["container_name"]] = dict(labels=value["labels"], running=True, restart=value["restart"],
                                                   mounts=mounts, entrypoint=value["entrypoint"],
                                                   deadline=value["environment"]["SNOW_EPOCH_EXPIRES_UNIX"])
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
