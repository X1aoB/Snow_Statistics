"""Manual offsets and collector-bound real lanes; synthetic defaults unchanged."""
import base64
import re
import subprocess
import time

from snow_statistics.landing import collector_identity as validate_collector_identity
from snow_statistics.landing import topics_for


def b64(value):
    return None if value is None else base64.b64encode(value).decode("ascii")


class KafkaSource:
    def __init__(self, bootstrap, group, *, source="synthetic", event_lane=None, collector_identity=None,
                 kafka_container=None):
        if not re.fullmatch(r"snow-ods-[a-z0-9-]{1,60}", group):
            raise ValueError("Use an independently owned snow-ods-* consumer group")
        self.bootstrap, self.group = bootstrap, group
        self.topics = topics_for(source, event_lane)
        self.collector = validate_collector_identity(collector_identity) if source == "real" else None
        if source != "real" and collector_identity is not None:
            raise ValueError("Synthetic Kafka must not claim a real collector")
        if kafka_container is not None and (source != "real" or
                not re.fullmatch(r"snow-real-[a-z][a-z0-9-]{2,31}-kafka", kafka_container)):
            raise ValueError("Only an explicit owned real epoch Kafka container may override Compose")
        self.kafka_container = kafka_container
        from kafka import KafkaAdminClient, KafkaConsumer
        self.admin = KafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=15000)
        try:
            self.consumer = KafkaConsumer(bootstrap_servers=bootstrap, group_id=group,
                                          enable_auto_commit=False, auto_offset_reset="none",
                                          allow_auto_create_topics=False, max_partition_fetch_bytes=1048576)
        except Exception:
            self.admin.close()
            raise

    def identity(self):
        # kafka-python 2.2.15 MetadataResponse stops at v7 (no TopicId).
        # Read IDs using the pinned broker's own 3.9.1 CLI instead of silently
        # treating delete/recreate of a topic as the same input stream.
        command = (["sudo", "docker", "exec", self.kafka_container] if self.kafka_container else
                   ["sudo", "docker", "compose", "--env-file", "lab/locks/images.env", "--env-file", "lab/.env",
                    "-f", "lab/compose.control.yaml", "exec", "-T", "kafka"])
        result = subprocess.run(command + ["/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", self.bootstrap,
                                 "--describe", "--topic", "(" + "|".join(re.escape(t) for t in self.topics) + ")"],
                                input="", capture_output=True, text=True, check=True, timeout=45)
        topics = dict(re.findall(r"Topic:\s+(\S+)\s+TopicId:\s+(\S+)", result.stdout))
        if set(topics) != set(self.topics):
            raise ValueError("Missing topic incarnation metadata")
        cluster = self.admin.describe_cluster()["cluster_id"]
        if not cluster:
            raise ValueError("Missing Kafka cluster ID")
        identity = dict(cluster_id=cluster, topic_ids=topics, group=self.group)
        if self.collector is not None:
            identity["collector"] = self.collector.copy()
        return identity

    @staticmethod
    def partition(key):
        from kafka import TopicPartition
        topic, partition = key.rsplit(":", 1)
        return TopicPartition(topic, int(partition))

    def bounds(self):
        from kafka import TopicPartition
        partitions = []
        for topic in self.topics:
            ids = self.consumer.partitions_for_topic(topic)
            if ids is None:
                raise ValueError("Missing required source topic")
            partitions.extend(TopicPartition(topic, p) for p in sorted(ids))
        self.consumer.assign(partitions)
        starts = self.consumer.beginning_offsets(partitions)
        ends = self.consumer.end_offsets(partitions)
        return {f"{p.topic}:{p.partition}": (starts[p], ends[p]) for p in partitions}

    def committed(self, key):
        return self.consumer.committed(self.partition(key))

    def read(self, key, start, end):
        if start == end:
            return
        tp = self.partition(key)
        self.consumer.assign([tp])
        self.consumer.seek(tp, start)
        remaining, deadline = end - start, time.monotonic() + 60
        while remaining:
            if time.monotonic() >= deadline:
                raise TimeoutError("Bounded Kafka capture timed out; no offset commit")
            for record in self.consumer.poll(timeout_ms=1000, max_records=min(500, remaining)).get(tp, []):
                if record.offset >= end:
                    raise ValueError("Unexpected record beyond frozen end offset")
                remaining -= 1
                yield dict(topic=record.topic, partition=record.partition, offset=record.offset,
                           timestamp_ms=record.timestamp, key_b64=b64(record.key), value_b64=b64(record.value),
                           headers=[[k, b64(v)] for k, v in record.headers])

    def commit(self, ends):
        from kafka.structs import OffsetAndMetadata
        self.consumer.commit({self.partition(k): OffsetAndMetadata(v, "snow-hdfs-ods-v1", -1) for k, v in ends.items()})

    def close(self):
        self.consumer.close(autocommit=False)
        self.admin.close()
