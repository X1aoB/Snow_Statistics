"""Windows-coordinated Hive operations; keep this console open for the private view."""
import argparse
import json
import os
import signal
import sys
from pathlib import Path

from snow_statistics.real_hive_dispatch import (
    OPERATIONS,
    HiveCoordinator,
    authority,
    frame_bytes,
    node_guard,
    read_frame,
    run_control,
    stop_worker,
    validate_envelope,
)
from snow_statistics.real_lab import private_relative, read_json, validate_config


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", help="Existing private runtime/real/config/<name>.json")
    parser.add_argument("--operation", choices=sorted(OPERATIONS))
    parser.add_argument("--run-id")
    parser.add_argument("--attempt")
    parser.add_argument("--evidence-sha256", help=argparse.SUPPRESS)
    parser.add_argument("--duration-seconds", type=int, default=900, help="Finite session, 60..1800 seconds")
    parser.add_argument("--node-phase", choices=("authority", "worker", "cancel"), help=argparse.SUPPRESS)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    if not 60 <= args.duration_seconds <= 1800:
        parser.error("Session duration must be 60..1800 seconds")
    if args.node_phase in {"worker", "cancel"}:
        node_guard(root, "snow-control")
        if any((args.config, args.operation, args.run_id, args.attempt, args.evidence_sha256)):
            raise ValueError("Control receives only its current metadata request")
        envelope = read_frame(sys.stdin.buffer)
        if args.node_phase == "worker":
            validate_envelope(envelope)
            def stopping(*_):
                raise SystemExit(143)
            for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
                signal.signal(signum, stopping)
            result = run_control(envelope, root)
        else:
            # Cancellation can occur after the request's expiry; ownership is
            # verified against its exact durable reservation before any stop.
            stop_worker(root, envelope)
            result = {"stopped": True}
        sys.stdout.buffer.write(frame_bytes(result))
        sys.stdout.buffer.flush()
        return
    if not args.config or not args.operation:
        parser.error("--config and --operation are required")
    private_relative(args.config, "config", ".json")
    if args.node_phase == "authority":
        node_guard(root, "snow-analysis")
        # Streamlit installs its own graceful TERM/INT handler after startup;
        # HUP and non-view phases still unwind the explicit runner context.
        def stopping(*_):
            raise SystemExit(143)
        for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            signal.signal(signum, stopping)
        authority(root, args.config, args.operation, args.run_id, args.attempt, args.evidence_sha256,
                  args.duration_seconds, sys.stdin.buffer, sys.stdout.buffer)
    else:
        if os.name != "nt" or args.evidence_sha256 or args.operation.startswith("lake-"):
            raise ValueError("Use the Windows coordinator; lake confirmation belongs to its own actual orchestrator")
        config = validate_config(read_json(root / args.config))
        if args.operation == "view":
            print("Private view: http://127.0.0.1:8502 — keep this console open; ending it closes the RPC gate.", flush=True)
        result = HiveCoordinator(config, args.config, root).run(args.operation, args.run_id,
                         attempt=args.attempt, duration_seconds=args.duration_seconds)
        print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
