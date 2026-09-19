"""Windows-coordinated, analysis-authorized Iceberg aggregate experiment."""
import argparse
import json
import os
import signal
import socket
from pathlib import Path

from snow_statistics.real_lab import REMOTE_ROOT, private_relative, read_json, validate_config
from snow_statistics.real_lake_authority import (
    accept,
    cleanup_copies,
    confirm,
    location,
    prepare,
    read,
    reserve,
)
from snow_statistics.real_lake_dispatch import LakeRunner, cancel_driver, execute, node_worker, verify


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--run-id", required=True, help="Already published and transferred aggregate-pair run")
    parser.add_argument("--attempt", required=True, help="New immutable lake attempt; never reuse partial table output")
    parser.add_argument("--recover-stages", action="store_true", help="Restore only this attempt's exact existing stage services; no execution or data deletion")
    parser.add_argument("--node-phase", choices=("prepare", "directories", "reserve", "accept", "execute", "verify", "confirm", "cancel-driver"), help=argparse.SUPPRESS)
    parser.add_argument("--evidence-sha256", help=argparse.SUPPRESS)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    private_relative(args.config, "config", ".json")
    config = validate_config(read_json(root / args.config))
    directory = location(root, config, args.run_id, args.attempt)
    if not args.node_phase:
        if os.name != "nt" or args.evidence_sha256:
            raise ValueError("Run the public coordinator on Windows; it owns the pinned-host VM transport")
        runner = LakeRunner(config, args.config, root)
        if args.recover_stages:
            runner.recover(args.run_id, args.attempt)
            result = dict(status="stages_restored")
        else:
            result = runner.lake(args.run_id, args.attempt)
    else:
        expected = "snow-analysis" if args.node_phase in {"prepare", "confirm"} else "snow-control"
        if os.name != "posix" or socket.gethostname() != expected or str(root) != REMOTE_ROOT or args.recover_stages:
            raise ValueError("Internal phase reached the wrong fixed host or checkout")
        def stopping(*_):
            raise SystemExit(143)
        for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            signal.signal(signum, stopping)
        if bool(args.evidence_sha256) != (args.node_phase == "confirm"):
            raise ValueError("Only the coordinator's confirmation step carries an observed evidence hash")
        if args.node_phase == "cancel-driver":
            result = cancel_driver(root, config, args.run_id, args.attempt)
        elif args.node_phase in {"execute", "verify"}:
            with node_worker(root, config, args.run_id, args.attempt, args.node_phase):
                cleanup_copies(root)
                result = {"execute": execute, "verify": verify}[args.node_phase](root, config, args.run_id, args.attempt)
        elif args.node_phase == "directories":
            cleanup_copies(root)
            directory.mkdir(parents=True, exist_ok=True)
            result = dict(metadata_directory=True, registered_payload=False)
        elif args.node_phase == "reserve":
            cleanup_copies(root)
            descriptor = json.loads(read(directory / "incoming-descriptor.json", 65536))
            reserve(root, config, args.run_id, args.attempt, descriptor)
            result = dict(reserved=True, expires_at=descriptor["expires_at"])
        elif args.node_phase == "confirm":
            cleanup_copies(root)
            result = confirm(root, config, args.run_id, args.attempt, args.evidence_sha256)
        else:
            cleanup_copies(root)
            action = {"prepare": prepare, "accept": accept, "execute": execute, "verify": verify}[args.node_phase]
            result = action(root, config, args.run_id, args.attempt)
    # No aggregates or collector identities are echoed by the public tool.
    print(json.dumps({key: result[key] for key in ("confirmed", "verified", "source", "input_origin", "expires_at", "status") if key in result}))


if __name__ == "__main__":
    main()
