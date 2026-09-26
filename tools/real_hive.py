"""Run the independent Hive aggregate phase on the configured transport VM.

No services are started/stopped by this CLI except its one ephemeral Spark
catalog container. Prepare the documented exclusive low-memory window first.
"""
import argparse
import json
import os
import socket
from contextlib import nullcontext
from datetime import UTC, datetime

from snow_statistics.io import write_json
from snow_statistics.landing import checked_receipt, load
from snow_statistics.lifecycle import timestamp
from snow_statistics.publication import publication_lock
from snow_statistics.real_backend_lifecycle import backend_adapters
from snow_statistics.real_hive import HiveRegistry, HiveRetention, SparkCatalog, register_release
from snow_statistics.real_lab import (
    ROOT,
    hdfs_context,
    job_data,
    no_input,
    private_relative,
    read_json,
    secret_file,
    validate_config,
    writer_registry,
)
from snow_statistics.real_publication import read_real_release
from snow_statistics.real_remote_lifecycle import RealRemoteLifecycle


def release_directory(config, run_id, root):
    """Analysis owns managed transfers, including explicitly synthetic fixtures."""
    if config["transport_node"] == "snow-analysis":
        from snow_statistics.real_transfer import paths
        directory = root / paths(config["lane"], run_id)["published"]
    else:
        directory = root / "runtime/real/publication"
    release = read_real_release(directory)
    if release["run_id"] != run_id:
        raise ValueError("Managed release is not the explicitly requested run")
    return directory


def execute(config, command, run_id=None, *, root=ROOT):
    if command not in {"register", "verify", "cleanup", "catalog-cleanup", "permit"}:
        raise ValueError("Unknown Hive operation")
    if os.name != "posix" or socket.gethostname() != config["transport_node"] or str(root) != "/home/snow/Snow_Statistics":
        raise ValueError("Run this CLI inside the configured transport VM at the fixed checkout path")
    if command in {"register", "verify", "permit"}:
        from snow_statistics.real_lab import metadata_paths
        metadata_paths(config, run_id)
    manager = RealRemoteLifecycle(root / "runtime/real/lifecycle" / config["lane"])
    ods = root / "runtime/real/ods" / config["lane"]
    state = load(ods / "state.json")
    if not manager.owner.exists():
        if state is None:
            return no_input()
        raise ValueError("Prepare the real lifecycle before registering Hive")
    owner, inventory = manager._read()
    expected_prefix = "hdfs://" + config["nodes"]["snow-control"] + ":9000/snow/"
    if (owner["roots"]["warehouse"] != expected_prefix + "warehouse/real/" + config["lane"] or
            owner["roots"]["ods"] != expected_prefix + "ods/real/kafka/" + config["lane"]):
        raise ValueError("Hive configuration does not match the registered real lane")
    registry = HiveRegistry(manager)
    catalog = SparkCatalog(root, registry, config["nodes"]["snow-control"])
    hive = HiveRetention(registry, catalog)
    # Separate outer operation lock: the remote manager takes its own lock.
    with publication_lock(manager.directory / "hive-operation"):
        if command == "catalog-cleanup":
            receipt = hive.purge_and_verify(inventory["backends"]["hive"]["resources"], datetime.now(UTC))
            return dict(source="real", input_origin=config["input_origin"], read_permission=False,
                        scope="Hive catalog only; HDFS and epoch cleanup not certified", receipt=receipt)
        if state is None:
            raise ValueError("Registered real resources require their durable ODS head")
        snapshot = checked_receipt(state)
        with hdfs_context(config) as (sink, hdfs):
            context = (backend_adapters(secret_file(root, config["backend_config_file"]), hdfs)
                       if config["backend_config_file"] else nullcontext({}))
            if config["input_origin"] == "real":
                from snow_statistics.real_quiescent import StoppedStorage
                writer = writer_registry(config, root)
                # Real phase is deliberately exclusive. Running engine checks
                # belong to their own existing phase, not this small Hive client.
                if timestamp(writer.epoch.read()["expires_at"]) <= datetime.now(UTC):
                    from snow_statistics.real_retired import RetiredStorage
                    context = nullcontext(RetiredStorage(writer.epoch, snapshot).adapters())
                else:
                    context = nullcontext(StoppedStorage(writer, snapshot).adapters())
            with context as checks:
                checks["hive"] = hive

                def cleanup():
                    # Probe even a never-initialized scope; otherwise a table
                    # created outside registration could evade the empty scope.
                    _, current = manager._read()
                    hive.purge_and_verify(current["backends"]["hive"]["resources"], datetime.now(UTC))
                    return manager.cleanup(hdfs, ods, sink, backend_checks=checks)

                receipt = cleanup()
                if command == "cleanup":
                    result = receipt
                elif command == "permit":
                    job, paths = job_data(config, root, run_id)
                    result = manager.issue_permit(job, root / paths["coverage"], None, root / paths["permit"],
                                                  hdfs, ods, sink, backend_checks=checks)
                else:
                    directory = release_directory(config, run_id, root)
                    result = register_release(directory, registry, catalog, cleanup, verify_only=command == "verify")
                envelope = dict(schema_version=1, source="real", input_origin=config["input_origin"],
                                command=command, run_id=run_id, result=result)
                write_json(manager.directory / "hive-last-operation.json", envelope)
                return envelope


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, help="Private runtime/real/config/<name>.json, same strict schema as real_lab")
    parser.add_argument("command", choices=["register", "verify", "cleanup", "catalog-cleanup", "permit"])
    parser.add_argument("--run-id", help="Existing managed release / real job ID for register, verify or permit")
    args = parser.parse_args()
    private_relative(args.config, "config", ".json")
    config = validate_config(read_json(secret_file(ROOT, args.config)))
    result = execute(config, args.command, args.run_id)
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))


if __name__ == "__main__":
    main()
