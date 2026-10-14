"""Standalone original-authority DIM verification. Default describes only."""
import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from snow_statistics.io import digest  # noqa: E402
from snow_statistics.publication import canonical  # noqa: E402
from snow_statistics.real_dimensions import check, execute, prepare, read, strict_json  # noqa: E402
from snow_statistics.real_lab import validate_config  # noqa: E402


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--prepare', action='store_true')
    group.add_argument('--check', action='store_true')
    group.add_argument('--execute', action='store_true')
    parser.add_argument('--config')
    parser.add_argument('--run-id')
    parser.add_argument('--attempt')
    args = parser.parse_args(argv)
    if not any((args.prepare, args.check, args.execute)):
        print(json.dumps(dict(mode='describe_only', input_read=False, files_written=False,
            vm_or_ssh_started=False, engine_acceptance_claimed=False,
            execution_node='original snow-analysis transport authority',
            output='registered private local validation only; no HDFS/model/public writes')))
        return 0
    if (not isinstance(args.config, str) or not re.fullmatch(r'runtime/real/config/[A-Za-z0-9_-]{1,100}\.json', args.config)
            or not args.run_id or not args.attempt):
        raise ValueError('Exact original ignored config, run ID and fresh attempt required')
    config = validate_config(strict_json(read(ROOT / args.config, 65536)))
    if args.prepare:
        result = prepare(ROOT, config, args.run_id, args.attempt)
    elif args.execute:
        result = execute(ROOT, config, args.run_id, args.attempt)
    else:
        descriptor, _ = check(ROOT, config, args.run_id, args.attempt)
        result = dict(source='real', checked=True, files_written=False, engine_acceptance_claimed=False,
                      descriptor_sha256=digest(canonical(descriptor)))
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
