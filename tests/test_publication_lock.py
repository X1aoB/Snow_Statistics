import errno
import os
import subprocess
import sys
import time
from types import SimpleNamespace

import pytest

from snow_statistics import publication
from snow_statistics.publication import PublicationLockBusy, publication_lock


@pytest.mark.parametrize("number", [errno.EAGAIN, errno.EWOULDBLOCK, errno.EBADF, errno.EIO])
def test_only_posix_lock_acquisition_contention_gets_dedicated_error(tmp_path, monkeypatch, number):
    original = OSError(number, "synthetic acquisition failure")

    def flock(*_):
        raise original

    monkeypatch.setattr(publication, "os", SimpleNamespace(name="posix"))
    monkeypatch.setitem(sys.modules, "fcntl", SimpleNamespace(flock=flock, LOCK_EX=2, LOCK_NB=4))
    expected = PublicationLockBusy if number in (errno.EAGAIN, errno.EWOULDBLOCK) else OSError
    with pytest.raises(expected) as caught, publication_lock(tmp_path):
        pytest.fail("The lock body must not run after acquisition fails")
    if expected is PublicationLockBusy:
        assert caught.value.errno == number and caught.value.__cause__ is original
    else:
        assert caught.value is original and not isinstance(caught.value, PublicationLockBusy)


def test_blocking_io_error_from_lock_body_is_not_reclassified_and_lock_is_released(tmp_path):
    original = BlockingIOError(errno.EAGAIN, "synthetic publisher body failure")
    with pytest.raises(BlockingIOError) as caught, publication_lock(tmp_path):
        raise original
    assert caught.value is original and not isinstance(caught.value, PublicationLockBusy)
    with publication_lock(tmp_path):
        pass


@pytest.mark.skipif(os.name != "posix", reason="Requires a real POSIX flock; exercised on Linux CI")
def test_actual_flock_process_contention_then_release(tmp_path):
    # The other process uses the OS primitive directly, not a mock of this module.
    script = """
import fcntl
import pathlib
import sys
directory = pathlib.Path(sys.argv[1])
with (directory / 'publisher.lock').open('a+b') as stream:
    fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
    (directory / 'ready').write_text('locked')
    sys.stdin.read(1)
"""
    child = subprocess.Popen([sys.executable, "-c", script, str(tmp_path)], stdin=subprocess.PIPE,
                             stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    try:
        deadline = time.monotonic() + 5
        while not (tmp_path / "ready").exists() and child.poll() is None and time.monotonic() < deadline:
            time.sleep(0.01)
        assert (tmp_path / "ready").exists(), "The lock-holding child did not become ready"
        with pytest.raises(PublicationLockBusy) as caught, publication_lock(tmp_path):
            pytest.fail("Concurrent acquisition must be refused")
        assert caught.value.errno in (errno.EAGAIN, errno.EWOULDBLOCK)
    finally:
        try:
            _, stderr = child.communicate(input="\n", timeout=5)
        except subprocess.TimeoutExpired:
            child.kill()
            child.communicate(timeout=5)
            raise
    assert child.returncode == 0, stderr
    with publication_lock(tmp_path):
        pass
