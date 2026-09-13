"""Staged capture / HDFS land / Kafka ACK; only this project's synthetic topics."""
import argparse
import json
from pathlib import Path

from snow_statistics.hdfs_landing import HdfsSink
from snow_statistics.kafka_landing import KafkaSource
from snow_statistics.landing import acknowledge, capture, land


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("capture", "land", "ack", "expire"))
    parser.add_argument("--lane", default="main-v1")
    parser.add_argument("--source", choices=("synthetic", "real"), default="synthetic")
    parser.add_argument("--event-lane", help="Explicit real sync replay lane; topic snow.real.<lane>.events.v1")
    parser.add_argument("--directory", type=Path, default=Path("runtime/ods/main-v1"))
    parser.add_argument("--max-records", type=int, default=10000)
    parser.add_argument("--fail-after-batch", action="store_true")
    parser.add_argument("--fail-after-commit", action="store_true")
    args = parser.parse_args()
    env = dict(line.split("=", 1) for line in Path("lab/.env").read_text().splitlines() if "=" in line)
    if args.phase in {"land", "expire"}:
        sink = HdfsSink(env["CONTROL_IP"], args.lane,
                        {"snow-compute": env["COMPUTE_IP"], "snow-analysis": env["ANALYSIS_IP"]}, source=args.source)
        try:
            if args.phase == "expire":
                from snow_statistics.real_ods import expire_window
                if args.source != "real":
                    parser.error("Only registered real ODS has automatic expiry")
                print(json.dumps(expire_window(args.directory, sink)))
                return
            receipt = land(args.directory, sink, args.fail_after_batch)
        finally:
            sink.client.close()
        print(json.dumps({k: receipt[k] for k in ("batch_id", "snapshot_id", "input", "offsets")}))
    else:
        source = KafkaSource(env["CONTROL_IP"] + ":9092", "snow-ods-" + args.lane, source=args.source, event_lane=args.event_lane)
        try:
            if args.phase == "capture":
                print(json.dumps({"batch_id": capture(args.directory, source, args.max_records, provenance=args.source,
                                                      event_lane=args.event_lane), "kafka_acked": False}))
            else:
                receipt = acknowledge(args.directory, source, args.fail_after_commit)
                print(json.dumps({"snapshot_id": receipt["snapshot_id"], "kafka_acked": True, "offsets": receipt["offsets"]}))
        finally:
            source.close()


if __name__ == "__main__":
    main()
