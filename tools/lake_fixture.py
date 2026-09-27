"""Explicit synthetic fixture helper; invoke only inside the owned Linux lab."""
import argparse
import json
import socket
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from snow_statistics.lake_fixture import initialize, land_and_reserve, lane_name, transfer  # noqa: E402


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("initialize", "land-permit", "export", "reserve", "accept"))
    parser.add_argument("--lane", required=True)
    args = parser.parse_args()
    lane_name(args.lane)
    expected = "snow-control" if args.phase == "export" else "snow-analysis"
    if ROOT != Path("/home/snow/Snow_Statistics") or socket.gethostname() != expected:
        raise ValueError("Use the exact owned lab checkout and phase's fixed node")
    if args.phase == "initialize":
        env = dict(line.split("=", 1) for line in (ROOT / "lab/.env").read_text().splitlines() if "=" in line)
        nodes = {"snow-" + key.lower(): env[key + "_IP"] for key in ("CONTROL", "COMPUTE", "ANALYSIS")}
        result = initialize(ROOT, args.lane, nodes)
    elif args.phase == "land-permit":
        result = land_and_reserve(ROOT, args.lane)
    else:
        result = transfer(ROOT, args.lane, args.phase)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
