"""Explicit real-only engine lifecycle. No command touches online collectors."""
import argparse
import json
from datetime import timedelta
from pathlib import Path

from snow_statistics.lifecycle import timestamp
from snow_statistics.real_epoch import (
    DockerEpoch,
    Epoch,
    expire_due,
    make_manifest,
    prepare,
    private_epoch_root,
    stop_all,
    supervise,
)

parser = argparse.ArgumentParser()
parser.add_argument("--root", required=True)
commands = parser.add_subparsers(dest="command", required=True)
init = commands.add_parser("prepare")
init.add_argument("--epoch", required=True)
init.add_argument("--original-at", required=True)
init.add_argument("--images-env", default="lab/locks/images.env")
init.add_argument("--analysis-ip")
init.add_argument("--jar")
init.add_argument("--fixture", action="store_true")
init.add_argument("--synthetic-engine-test", action="store_true")
for name in ("start", "stop", "retire", "expire-fixture"):
    sub = commands.add_parser(name)
    sub.add_argument("--epoch", required=True)
    if name == "start":
        sub.add_argument("--stage", choices=["storage", "realtime"], default="storage")
commands.add_parser("cleanup")
commands.add_parser("watch")
commands.add_parser("stop-all")
args = parser.parse_args()
root = private_epoch_root(args.root)
if args.command == "prepare":
    selected = {"PYTHON_IMAGE"} if args.fixture else {"KAFKA_IMAGE", "FLINK_IMAGE", "DORIS_FE_IMAGE", "DORIS_BE_IMAGE"}
    images = dict(line.split("=", 1) for line in Path(args.images_env).read_text().splitlines() if "=" in line and not line.startswith("#"))
    manifest = make_manifest(args.epoch, args.original_at, {key: images[key] for key in selected},
                             fixture=args.fixture, synthetic_engine_test=args.synthetic_engine_test)
    result = prepare(root / args.epoch, manifest, args.analysis_ip, args.jar)
else:
    docker = DockerEpoch()
    if args.command == "watch":
        supervise(root, docker)
        result = {"stopped": True}
    elif args.command == "cleanup":
        result = expire_due(root, docker)
    elif args.command == "stop-all":
        result = stop_all(root, docker)
    else:
        epoch = Epoch(root / args.epoch, docker)
        if args.command == "start":
            expire_due(root, docker)  # All registered expired epochs must be gone first.
            result = epoch.start(args.stage)
        elif args.command == "stop":
            result = epoch.stop()
        elif args.command == "expire-fixture":
            result = epoch.retire(now=timestamp(epoch.read()["expires_at"]) + timedelta(seconds=1), fixture_clock=True)
        else:
            result = epoch.retire()
print(json.dumps(result, sort_keys=True))
