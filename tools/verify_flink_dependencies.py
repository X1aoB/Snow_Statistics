"""Verify the resolved Maven runtime JAR set; the provided Flink runtime is image-pinned."""
import argparse
import hashlib
import json
import os
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--classpath", type=Path, required=True)
parser.add_argument("--repository", type=Path, default=Path.home() / ".m2/repository")
parser.add_argument("--record", action="store_true", help="Explicitly record a reviewed dependency update")
args = parser.parse_args()
root = Path(__file__).resolve().parents[1]
repository = args.repository.resolve()
artifacts = {}
for entry in args.classpath.read_text().strip().split(os.pathsep):
    path = Path(entry).resolve()
    relative = path.relative_to(repository).as_posix()
    if path.suffix != ".jar":
        raise ValueError("Expected Maven JAR")
    with path.open("rb") as stream:
        artifacts[relative] = hashlib.file_digest(stream, "sha256").hexdigest()
if not artifacts:
    raise ValueError("Empty dependency set")
lock = root / "lab/locks/flink-jars.json"
result = dict(scope="Maven runtime dependencies; provided Flink/JDK are covered by the pinned Flink image", artifacts=dict(sorted(artifacts.items())))
if args.record:
    lock.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8", newline="\n")
elif json.loads(lock.read_bytes()) != result:
    raise ValueError("Resolved Flink dependencies differ from the reviewed lock")
print(f"Verified {len(artifacts)} resolved runtime JARs")
