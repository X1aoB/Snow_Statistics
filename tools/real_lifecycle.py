"""Operate one registered real remote scope; initialized backend checks fail closed.

The CLI never accepts fabricated Kafka/Doris/Checkpoint success flags. A runner
with these initialized resources must call RealRemoteLifecycle with actual
backend adapters before obtaining a permit.
"""
import argparse
import json
from contextlib import nullcontext
from pathlib import Path
from urllib.parse import urlsplit

from snow_statistics.hdfs_landing import HdfsSink
from snow_statistics.real_remote_lifecycle import RealRemoteLifecycle, WebHdfsOwned

parser = argparse.ArgumentParser()
parser.add_argument("--registry", required=True)
commands = parser.add_subparsers(dest="command", required=True)
initialize = commands.add_parser("init")
for key in ("warehouse-root", "auxiliary-root", "ods-root", "instance-id", "generation"):
    initialize.add_argument("--" + key, required=True)
register = commands.add_parser("register")
register.add_argument("--path", required=True)
register.add_argument("--kind", required=True, choices=["raw", "request_detail", "auxiliary", "aggregate"])
register.add_argument("--original-at", required=True)
reserve = commands.add_parser("reserve-job")
reserve.add_argument("--job-file", required=True)
reserve.add_argument("--original-raw-at", required=True)
reserve.add_argument("--original-auxiliary-at", required=True)
registered_backend = commands.add_parser("register-backend")
registered_backend.add_argument("--backend", required=True, choices=["kafka", "doris", "checkpoint", "hive"])
registered_backend.add_argument("--resource", required=True)
registered_backend.add_argument("--kind", required=True, choices=["raw", "request_detail", "auxiliary", "aggregate"])
registered_backend.add_argument("--original-at", required=True)
for command in ("cleanup", "permit"):
    sub = commands.add_parser(command)
    sub.add_argument("--ods-directory", required=True)
    sub.add_argument("--datanodes-file", required=True, help="JSON mapping of configured DataNode hostnames to this lab's IPs")
    sub.add_argument("--backend-config", help="Private precise resource configuration for actual backend API/SQL checks")
    if command == "permit":
        sub.add_argument("--job-file", required=True)
        sub.add_argument("--coverage-file", required=True)
        sub.add_argument("--auxiliary-file")
        sub.add_argument("--permit-file", required=True)
args = parser.parse_args()
manager = RealRemoteLifecycle(args.registry)
if args.command == "init":
    manager.initialize(args.warehouse_root, args.auxiliary_root, args.ods_root, args.instance_id, args.generation)
    result = {"source": "real", "initialized": True}
elif args.command == "register":
    result = {"registered": manager.register(args.path, args.kind, args.original_at)}
elif args.command == "reserve-job":
    result = manager.reserve_job(json.loads(Path(args.job_file).read_bytes()), args.original_raw_at, args.original_auxiliary_at)
elif args.command == "register-backend":
    manager.register_backend(args.backend, args.resource, args.kind, args.original_at)
    result = {"registered_backend": args.backend, "resource": args.resource}
else:
    owner, _ = manager._read()
    location = urlsplit(owner["roots"]["ods"])
    nodes = json.loads(Path(args.datanodes_file).read_bytes())
    sink = HdfsSink(location.hostname, location.path.rsplit("/", 1)[-1], nodes, source="real")
    try:
        hdfs = WebHdfsOwned(sink)
        if args.backend_config:
            from snow_statistics.real_backend_lifecycle import backend_adapters
            adapters = backend_adapters(args.backend_config, hdfs)
        else:
            adapters = nullcontext({})
        with adapters as checks:
            if args.command == "cleanup":
                result = manager.cleanup(hdfs, args.ods_directory, sink, backend_checks=checks)
            else:
                result = manager.issue_permit(json.loads(Path(args.job_file).read_bytes()), args.coverage_file, args.auxiliary_file,
                                              args.permit_file, hdfs, args.ods_directory, sink, backend_checks=checks)
    finally:
        sink.client.close()
print(json.dumps(result, sort_keys=True))
