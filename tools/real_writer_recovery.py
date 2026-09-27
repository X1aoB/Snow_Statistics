"""Run on snow-analysis: actual pause/resume, no user-supplied checkpoint path."""
import argparse
import json

from snow_statistics.real_writer import ActualWriter, failure_metadata
from snow_statistics.real_writer_recovery import WriterRecovery

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--root", required=True)
parser.add_argument("--epoch", required=True)
parser.add_argument("--collector-url", required=True)
parser.add_argument("--reader-token-file", required=True)
parser.add_argument("action", choices=("pause", "resume"))
args = parser.parse_args()
try:
    writer = ActualWriter(args.root, args.epoch, args.collector_url, args.reader_token_file)
    recovery = WriterRecovery(writer)
    result = recovery.pause() if args.action == "pause" else recovery.resume()
    print(json.dumps(dict(complete=True, source="real", action=args.action, epoch_id=args.epoch,
                         sequence=result["sequence"], checkpoint_payload_copied=False)))
except Exception as error:
    # WriterRecovery stops its epoch after mutation failure. Lock contention is
    # deliberately not followed by another stop, which would interrupt sync.
    print(json.dumps(failure_metadata(error)))
    raise SystemExit(1) from None
