import sys
from types import SimpleNamespace

import pytest
from test_real_ods import COLLECTOR

from snow_statistics.kafka_landing import KafkaSource
from snow_statistics.landing import collector_identity


@pytest.mark.parametrize("value", [None, {}, COLLECTOR | {"schema_version": True}, COLLECTOR | {"source": "synthetic"},
                                   COLLECTOR | {"generation": "not-uuid"}, COLLECTOR | {"private": "never-copy"}])
def test_real_kafka_requires_exact_collector_identity_before_network(value):
    with pytest.raises(ValueError):
        KafkaSource("example.invalid:9092", "snow-ods-fixture", source="real", collector_identity=value)


@pytest.mark.parametrize("container", ["kafka", "snow-lab-control-kafka-1", "snow-real-x-kafka", "snow-real-foo-kafka;whoami", "../snow-real-foo-kafka"])
def test_epoch_container_override_cannot_escape_owned_name(container):
    with pytest.raises(ValueError, match="owned real epoch"):
        KafkaSource("example.invalid:9092", "snow-ods-fixture", source="real", collector_identity=COLLECTOR, kafka_container=container)


def test_identity_carries_an_immutable_collector_and_uses_exact_epoch_cli(monkeypatch):
    commands = []
    admin = SimpleNamespace(describe_cluster=lambda: {"cluster_id": "fixture-cluster"}, close=lambda: None)
    consumer = SimpleNamespace(close=lambda **_: None)
    monkeypatch.setitem(sys.modules, "kafka", SimpleNamespace(KafkaAdminClient=lambda **_: admin, KafkaConsumer=lambda **_: consumer))

    def run(command, **options):
        commands.append(command)
        topic = "snow.real.fixture.events.v1" if "exec" in command and "compose" not in command else "snow.synthetic.events.v1"
        extra = "" if topic.startswith("snow.real") else "\n".join(
            "Topic: snow.synthetic.cdc.snow_ops." + name + " TopicId: id_" + name for name in ("campaigns", "contents", "tickets"))
        return SimpleNamespace(stdout="Topic: " + topic + " TopicId: fixture_topic_id\n" + extra)

    monkeypatch.setattr("snow_statistics.kafka_landing.subprocess.run", run)
    supplied = collector_identity(COLLECTOR)
    real = KafkaSource("example.invalid:9092", "snow-ods-fixture", source="real", event_lane="fixture",
                       collector_identity=supplied, kafka_container="snow-real-fixture-run-kafka")
    supplied["generation"] = "59d28bda-f5c8-4ec0-9ea4-031f3a580f34"
    assert real.identity()["collector"] == COLLECTOR
    assert commands[-1][:4] == ["sudo", "docker", "exec", "snow-real-fixture-run-kafka"]
    assert real.identity()["topic_ids"] == {"snow.real.fixture.events.v1": "fixture_topic_id"}
    synthetic = KafkaSource("example.invalid:9092", "snow-ods-fixture")
    assert "collector" not in synthetic.identity()
    assert commands[-1][:3] == ["sudo", "docker", "compose"]
