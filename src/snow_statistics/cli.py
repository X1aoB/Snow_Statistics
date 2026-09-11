import argparse
import json
import os
import shutil
import time
from pathlib import Path

from .io import write_json
from .model import build
from .simulator import generate


def main():
    parser = argparse.ArgumentParser(description="Snow Statistics: independent analytics and synthetic laboratory")
    commands = parser.add_subparsers(dest="command", required=True)
    serve = commands.add_parser("serve")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8100)
    for name in ("simulate", "demo"):
        cmd = commands.add_parser(name)
        cmd.add_argument("--seed", type=int, default=42)
        cmd.add_argument("--users", type=int, default=100)
        cmd.add_argument("--output", type=Path, default=Path("runtime/demo"))
    model = commands.add_parser("model")
    model.add_argument("input", type=Path)
    model.add_argument("--output", type=Path, default=Path("runtime/model.json"))
    sync = commands.add_parser("sync")
    sync.add_argument("--url", required=True)
    sync.add_argument("--bootstrap", default="localhost:9092")
    sync.add_argument("--directory", type=Path, default=Path("runtime/sync"))
    sync.add_argument("--lane", help="Optional isolated replay lane; use a fresh sync directory")
    sync.add_argument("--source", choices=("real", "synthetic"))
    sync.add_argument("--follow", action="store_true", help="Keep polling; a transport error exits for supervised recovery")
    sync.add_argument("--poll-seconds", type=float, default=1.0)
    bench = commands.add_parser("benchmark")
    bench.add_argument("--events", type=int, choices=(100_000, 1_000_000), default=100_000)
    bench.add_argument("--output", type=Path, default=Path("runtime/benchmark.json"))
    args = parser.parse_args()
    if args.command == "serve":
        import uvicorn
        uvicorn.run("snow_statistics.api:create_app", factory=True, host=args.host, port=args.port, workers=1,
                    access_log=False, limit_concurrency=32, timeout_keep_alive=5)
    elif args.command in {"simulate", "demo"}:
        if not 1 <= args.users <= 100_000:
            parser.error("users must be 1..100000")
        fixture = generate(args.seed, args.users)
        write_json(args.output / "fixture.json", fixture)
        if args.command == "demo":
            result = build(fixture)
            write_json(args.output / "model.json", result)
            print(json.dumps(result["quality"]))
        print(f"Synthetic artifacts: {args.output}")
    elif args.command == "model":
        result = build(json.loads(args.input.read_text(encoding="utf-8")))
        write_json(args.output, result)
        print(json.dumps(result["quality"]))
    elif args.command == "sync":
        from .sync import kafka_sync
        count = kafka_sync(args.url, os.getenv("SNOW_READER_TOKEN", ""), args.bootstrap, args.directory,
                           lane=args.lane, source=args.source, follow=args.follow, poll_seconds=args.poll_seconds)
        print(json.dumps({"published": count}))
    elif args.command == "benchmark":
        if shutil.disk_usage(Path.cwd()).free < 35 * 1024**3:
            parser.error("host free disk below 35 GiB gate")
        if args.events == 1_000_000:
            previous = json.loads(args.output.read_text()) if args.output.exists() else {}
            if previous.get("events") != 100_000 or previous.get("quality", {}).get("quarantined") != 0:
                parser.error("complete the 100000 event gate at this output path first")
        start = time.perf_counter()
        fixture = generate(users=args.events // 7 + 1)
        fixture["events"] = fixture["events"][:args.events]
        result = build(fixture)
        report = dict(engine="python correctness oracle (not distributed)", events=len(fixture["events"]),
                      seconds=round(time.perf_counter() - start, 3), quality=result["quality"],
                      source="synthetic", seed=42, free_disk_bytes=shutil.disk_usage(Path.cwd()).free)
        write_json(args.output, report)
        print(json.dumps(report))


if __name__ == "__main__":
    main()
