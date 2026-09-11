"""Actual leader/ISR/quorum exercise. Unacknowledged writes remain explicitly uncertain."""
import argparse
import json
import time

from ha_lab import Lab
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError, NotEnoughReplicasAfterAppendError, NotEnoughReplicasError

from snow_statistics.io import write_json

parser = argparse.ArgumentParser()
parser.add_argument("--lane", required=True)
args = parser.parse_args()
lab = Lab(args.lane, "kafka")
bootstrap = [lab.host + ":" + str(port) for port in (19092, 19093, 19094)]
topic = "snow.synthetic.ha." + args.lane
strict = topic + ".strict"
admin = None
history, acknowledged = [], []


def metadata(name):
    try:
        partitions = admin.describe_topics([name])[0]["partitions"]
        return partitions[0] if partitions else dict(leader=-1, isr=[])
    except KafkaError:
        return dict(leader=-1, isr=[])


def send(name, label):
    producer = KafkaProducer(bootstrap_servers=bootstrap, acks="all", retries=0,
                             max_in_flight_requests_per_connection=1, request_timeout_ms=6000,
                             max_block_ms=6000)
    try:
        return producer.send(name, partition=0, value=label.encode()).get(timeout=12).offset
    finally:
        producer.close(timeout=1)


def read(name):
    consumer = KafkaConsumer(bootstrap_servers=bootstrap, enable_auto_commit=False)
    try:
        part = TopicPartition(name, 0)
        consumer.assign([part])
        begin, end = consumer.beginning_offsets([part])[part], consumer.end_offsets([part])[part]
        assert begin == 0
        consumer.seek(part, 0)
        rows, deadline = [], time.monotonic() + 30
        while consumer.position(part) < end:
            if time.monotonic() > deadline:
                raise RuntimeError("Bounded readback deadline")
            for values in consumer.poll(timeout_ms=500).values():
                rows.extend(dict(offset=r.offset, value=r.value.decode()) for r in values if r.offset < end)
        return rows
    finally:
        consumer.close()


try:
    def ready():
        global admin
        try:
            admin = KafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=5000)
            if len(admin.describe_cluster()["brokers"]) == 3:
                return True
            admin.close()
            admin = None
            return False
        except KafkaError:
            if admin:
                admin.close()
                admin = None
            return False
    lab.until(ready, "three KRaft brokers ready", 180)
    assert topic not in admin.list_topics() and strict not in admin.list_topics()
    admin.create_topics([NewTopic(name, 1, 3, topic_configs={"min.insync.replicas": minimum,
                         "retention.ms": "-1", "retention.bytes": "-1"}) for name, minimum in ((topic, "2"), (strict, "3"))])
    lab.until(lambda: len(metadata(topic)["isr"]) == 3, "initial ISR=3")
    history.append(dict(phase="initial", **metadata(topic)))
    acknowledged.append(dict(value="before", offset=send(topic, "before")))
    send(strict, "strict-before")
    lab.observe("before")
    first = metadata(topic)["leader"]
    assert first in (1, 2, 3)
    lab.node("stop", "broker", first)
    lab.until(lambda: metadata(topic)["leader"] != first and len(metadata(topic)["isr"]) == 2,
              "leader re-election with ISR=2")
    history.append(dict(phase="one_down", **metadata(topic)))
    acknowledged.append(dict(value="one-down", offset=send(topic, "one-down")))
    lab.until(lambda: len(metadata(strict)["isr"]) == 2, "strict probe ISR=2")
    try:
        send(strict, "strict-refused")
    except (NotEnoughReplicasError, NotEnoughReplicasAfterAppendError) as error:
        strict_error = type(error).__name__
    else:
        raise RuntimeError("min ISR=3 probe unexpectedly acknowledged with ISR=2")
    # Combined broker/controller loss also removes quorum. Do not conflate this
    # case with the independently observed min-ISR rejection above.
    leader = metadata(topic)["leader"]
    second = next(n for n in (1, 2, 3) if n not in (first, leader))
    lab.node("stop", "broker", second)
    time.sleep(3)
    try:
        send(topic, "two-down-unacknowledged")
    except KafkaError as error:
        quorum_error = type(error).__name__
    else:
        raise RuntimeError("Two lost combined nodes unexpectedly acknowledged a write")
    write_json(lab.folder / "fault.json", dict(first_stopped=first, second_stopped=second,
               strict_error=strict_error, two_down_error=quorum_error, acknowledged=acknowledged,
               note="Timeout/no acknowledgement does not establish that a record was never appended"))
    lab.node("start", "broker", first)
    lab.node("start", "broker", second)
    lab.until(lambda: len(metadata(topic)["isr"]) == 3 and len(metadata(strict)["isr"]) == 3,
              "restored ISR=3", 180)
    history.append(dict(phase="restored", **metadata(topic)))
    acknowledged.append(dict(value="restored", offset=send(topic, "restored")))
    rows, strict_rows = read(topic), read(strict)
    for item in acknowledged:
        assert rows[item["offset"]] == item
    assert all(r["value"] in {"before", "one-down", "restored", "two-down-unacknowledged"} for r in rows)
    assert sum(r["value"] == "two-down-unacknowledged" for r in rows) <= 1
    assert strict_rows[0]["value"] == "strict-before"
    assert all(r["value"] in {"strict-before", "strict-refused"} for r in strict_rows)
    lab.observe("after")
    write_json(lab.folder / "accepted.json", dict(source="synthetic", lane=args.lane, kafka="3.9.1",
              metadata=history, acknowledged=acknowledged, records=rows, strict_records=strict_rows,
              strict_error=strict_error, quorum_error=quorum_error, all_acknowledged_retained=True,
              unacknowledged_visible=sum(r["value"] == "two-down-unacknowledged" for r in rows),
              scope="Three combined KRaft broker/controller processes on one VM/physical host; no ZooKeeper dependency or physical disaster recovery"))
    print(json.dumps(dict(leader_changed=first != history[1]["leader"], strict_error=strict_error,
                         quorum_error=quorum_error, records=len(rows), all_acknowledged_retained=True)), flush=True)
finally:
    if admin:
        admin.close()
    lab.stop()
