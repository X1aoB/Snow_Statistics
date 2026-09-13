"""Production real writer bootstrap; run on the isolated analysis VM."""
import argparse
import json

from snow_statistics.real_writer import ActualWriter, failure_metadata

parser = argparse.ArgumentParser()
parser.add_argument("--root", required=True)
parser.add_argument("--epoch", required=True)
parser.add_argument("--collector-url", required=True)
parser.add_argument("--reader-token-file", required=True)
parser.add_argument("--release-directory", help="Managed aggregate-only pair, required for publish")
parser.add_argument("action", choices=("initialize", "submit", "publish", "status"))
args = parser.parse_args()
writer = None
try:
    writer = ActualWriter(args.root, args.epoch, args.collector_url, args.reader_token_file)
    if args.action == "initialize":
        result = writer.initialize()
    elif args.action == "submit":
        result = writer.submit()
    elif args.action == "publish":
        if not args.release_directory:
            raise ValueError("A managed real release directory is required")
        result = writer.publish(args.release_directory)
    else:
        result = writer.registry.ready(writer.identity())
    print(json.dumps(dict(complete=True, source="real", action=args.action, epoch_id=args.epoch,
                         registered=True, result_keys=sorted(result))))
except Exception as error:
    result = failure_metadata(error)
    if writer is not None and args.action != "status":
        try:
            writer.epoch.stop()
            result["owned_readers_stopped"] = True
        except Exception:
            result["owned_readers_stopped"] = False
    print(json.dumps(result))
    raise SystemExit(1) from None
