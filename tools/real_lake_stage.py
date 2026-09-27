"""Internal exact existing-container phase helper; invoked by the Windows coordinator."""
import argparse
import json
import os
import socket
from pathlib import Path

from snow_statistics.real_lab import REMOTE_ROOT, private_relative, read_json, secret_file, validate_config
from snow_statistics.real_lake_stage import SERVICES, reserve_stage, switch_stage


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--attempt", required=True)
    parser.add_argument("--stage", choices=("reserve", "yarn", "restore"), required=True)
    args = parser.parse_args()
    root, node = Path(__file__).resolve().parents[1], socket.gethostname()
    if os.name != "posix" or str(root) != REMOTE_ROOT or node not in SERVICES:
        raise ValueError("Only the fixed existing VM checkout may handle a stage operation")
    private_relative(args.config, "config", ".json")
    config = validate_config(read_json(secret_file(root, args.config)))
    if args.stage == "reserve":
        reserve_stage(root, config, args.run_id, args.attempt, node)
    else:
        switch_stage(root, config, args.run_id, args.attempt, node, restore=args.stage == "restore")
    print(json.dumps(dict(node=node, stage=args.stage, checked=True)))


if __name__ == "__main__":
    main()
