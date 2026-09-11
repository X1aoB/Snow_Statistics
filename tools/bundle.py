"""Package only Git-indexed source files; excluded runtime/credentials cannot enter."""
import subprocess
import tarfile
from pathlib import Path

root = Path(__file__).resolve().parents[1]
files = subprocess.run(["git", "ls-files", "-z"], cwd=root, capture_output=True, check=True).stdout.decode().split("\0")
output = root / "runtime/source.tar.gz"
output.parent.mkdir(exist_ok=True)
with tarfile.open(output, "w:gz") as archive:
    for name in files:
        if name:
            archive.add(root / name, arcname=name)
print(output)
