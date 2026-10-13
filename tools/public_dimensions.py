"""Build immutable, public-metadata-only DIM artifacts. Default: describe only."""
import argparse
import json
from pathlib import Path

from snow_statistics.io import write_json
from snow_statistics.public_dimensions import build_bundle, read_spec


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, help="Public catalogue/receipt metadata specification")
    parser.add_argument("--execute", action="store_true", help="Write a fresh source-isolated artifact")
    parser.add_argument("--check", action="store_true", help="Validate input without writing")
    args = parser.parse_args(argv)
    if args.check and args.execute:
        parser.error("Choose check or execute")
    if not args.check and not args.execute:
        print(json.dumps(dict(mode="describe", input_read=False, files_written=False,
                              output="runtime/public-dimensions/<source>/<artifact_id>/",
                              events_read=False, services_started=False, engine_acceptance=False)))
        return 0
    if args.input is None:
        parser.error("--input is required for check/execute")
    value = build_bundle(read_spec(args.input))
    result = dict(mode="execute" if args.execute else "check", artifact_id=value["artifact_id"],
                  source=value["source"], releases=len(value["releases"]),
                  dimension_rows=len(value["dimensions"]), coverage_segments=len(value["coverage"]),
                  files_written=False, engine_acceptance=False)
    if args.execute:
        root = Path(__file__).resolve().parents[1]
        parent = root / "runtime" / "public-dimensions" / value["source"]
        # Resolve each ancestor: metadata artifacts must stay in this checkout's
        # ignored runtime, even when an existing parent is a symlink/junction.
        if not parent.resolve().is_relative_to((root / "runtime").resolve()):
            raise ValueError("DIM artifact parent escaped project runtime")
        target = parent / value["artifact_id"]
        target.mkdir(parents=True, exist_ok=False)
        write_json(target / "dimensions.json", value)
        write_json(target / "manifest.json", result | {"files_written": True,
                   "contains_fact_rows": False, "retention_class": "public_dimension_metadata",
                   "partial_directory_is_not_success": True})
        result["files_written"] = True
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
