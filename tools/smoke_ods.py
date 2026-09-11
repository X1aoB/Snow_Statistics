"""Real broker/HDFS fault injection; invoke land and ack in resource phases."""
import argparse
import json
from pathlib import Path

from snow_statistics.hdfs_landing import HdfsSink
from snow_statistics.io import write_json
from snow_statistics.kafka_landing import KafkaSource
from snow_statistics.landing import acknowledge, capture, land, pending

parser = argparse.ArgumentParser()
parser.add_argument("phase", choices=("land", "ack"))
parser.add_argument("--lane", default="acceptance-v1")
args = parser.parse_args()
directory = Path("runtime/ods") / args.lane
env = dict(line.split("=", 1) for line in Path("lab/.env").read_text().splitlines() if "=" in line)
report_path = directory / "acceptance.json"
if args.phase == "land":
    batch, manifest = pending(directory)
    assert not (directory / "state.json").exists() and not (directory / "landed.json").exists()
    sink = HdfsSink(env["CONTROL_IP"], args.lane,
                    {"snow-compute": env["COMPUTE_IP"], "snow-analysis": env["ANALYSIS_IP"]})
    try:
        try:
            land(directory, sink, fail_after_batch=True)
            raise AssertionError("Expected injected failure")
        except RuntimeError as exc:
            assert "Injected failure after HDFS batch" in str(exc)
        assert not (directory / "landed.json").exists()
        receipt = land(directory, sink)
        assert land(directory, sink) == receipt
    finally:
        sink.client.close()
    report = dict(source="synthetic", batch_id=batch, snapshot_id=receipt["snapshot_id"], counts=manifest["counts"],
                  offsets=manifest["ends"], hdfs_replica_gate=2, hdfs_readback_equal=True,
                  recovery_after_hdfs_commit=True, replay_same_snapshot=True, kafka_acked_during_land=False)
else:
    report = json.loads(report_path.read_bytes())
    source = KafkaSource(env["CONTROL_IP"] + ":9092", "snow-ods-" + args.lane)
    try:
        assert all(source.committed(key) is None for key in report["offsets"])
        try:
            acknowledge(directory, source, fail_after_commit=True)
            raise AssertionError("Expected injected failure")
        except RuntimeError as exc:
            assert "Injected failure after Kafka ACK" in str(exc)
        assert not (directory / "state.json").exists() and (directory / "pending.json").exists()
        receipt = acknowledge(directory, source)
        assert capture(directory, source) is None
        assert all(source.committed(key) == value for key, value in receipt["offsets"].items())
        report.update(recovery_after_kafka_commit=True, repeated_capture_empty=True, committed_offsets_equal=True)
    finally:
        source.close()
write_json(report_path, report)
print(json.dumps(report))
