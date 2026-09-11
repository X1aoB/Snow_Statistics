import hashlib
import json
import os
from contextlib import contextmanager
from pathlib import Path


def atomic_write(path: Path, content: bytes):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    with temp.open("wb") as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temp, path)
    if os.name != "nt":
        fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def write_json(path, value):
    atomic_write(Path(path), json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2).encode())


def digest(content):
    return hashlib.sha256(content).hexdigest()


@contextmanager
def exclusive(directory):
    directory.mkdir(parents=True, exist_ok=True)
    lock = directory / "writer.lock"
    fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        os.write(fd, str(os.getpid()).encode())
        os.close(fd)
        yield
    finally:
        lock.unlink(missing_ok=True)
