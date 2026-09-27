"""Synthetic Docker replies only; absence never implies ownership of a live object."""
import subprocess
from types import SimpleNamespace

import pytest

from snow_statistics import real_hive_dispatch as hive
from snow_statistics import real_lake_dispatch as lake
from snow_statistics import real_offline_small as small
from snow_statistics.docker_absence import inspect_missing
from snow_statistics.io import write_json

CID = "a" * 64
HIVE = "snow-real-hive-" + "b" * 20
LAKE = "snow-real-lake-" + "c" * 20


def missing(expected, *, formatted, prefix=b"error: no such object: "):
    return SimpleNamespace(returncode=1, stdout=b"\n" if formatted else b"[]\n",
                           stderr=prefix + expected.encode() + b"\n")


@pytest.mark.parametrize("expected", [CID, HIVE, LAKE])
@pytest.mark.parametrize("formatted", [True, False])
@pytest.mark.parametrize("prefix", [b"error: no such object: ", b"Error: No such object: ",
                                    b"Error response from daemon: No such container: "])
def test_known_exact_missing_shapes(expected, formatted, prefix):
    assert inspect_missing(missing(expected, formatted=formatted, prefix=prefix), expected, formatted=formatted)


@pytest.mark.parametrize("formatted", [True, False])
@pytest.mark.parametrize("mutation", ["wrong-id", "abbreviated", "connection", "permission", "trailing",
                                      "success", "exit-two", "boolean-exit", "wrong-stdout", "oversize",
                                      "text-output", "json-object", "extra-result", "wrong-type"])
def test_unknown_scope_or_transport_is_never_absent(formatted, mutation):
    value = missing(CID, formatted=formatted)
    if mutation == "wrong-id":
        value.stderr = value.stderr.replace(CID.encode(), b"d" * 64)
    elif mutation == "abbreviated":
        value.stderr = value.stderr.replace(CID.encode(), CID[:12].encode())
    elif mutation == "connection":
        value.stderr = b"Cannot connect to the Docker daemon"
    elif mutation == "permission":
        value.stderr = b"permission denied; " + value.stderr
    elif mutation == "trailing":
        value.stderr += b"second error\n"
    elif mutation in {"success", "exit-two", "boolean-exit"}:
        value.returncode = {"success": 0, "exit-two": 2, "boolean-exit": True}[mutation]
    elif mutation == "wrong-stdout":
        value.stdout = b"[]\n" if formatted else b"\n"
    elif mutation == "oversize":
        value.stdout = b" " * 100
    elif mutation == "text-output":
        value.stderr = value.stderr.decode()
    elif mutation == "json-object":
        value.stdout = b"{}\n"
    elif mutation == "extra-result":
        value.stdout = b"[]\n[]\n"
    elif mutation == "wrong-type":
        value.stderr = value.stderr.replace(b"object", b"image")
    assert not inspect_missing(value, CID, formatted=formatted)


@pytest.mark.parametrize("expected", ["", "snow-spark-yarn", CID[:12], "../foreign", HIVE + "\n", None])
def test_caller_must_supply_fixed_exact_scope(expected):
    with pytest.raises(ValueError, match="exact registered"):
        inspect_missing(missing(CID, formatted=True), expected, formatted=True)


def test_offline_cleanup_accepts_actual_lowercase_format(tmp_path, monkeypatch):
    cid = tmp_path / "runtime/real/runs/synthetic-01/data/daily.cid"
    cid.parent.mkdir(parents=True)
    cid.write_text(CID)
    calls = []
    def run(command, **kwargs):
        calls.append(command)
        return missing(CID, formatted=True)
    monkeypatch.setattr(small.subprocess, "run", run)
    small.driver_cleanup(tmp_path, "daily", "synthetic-01")
    assert len(calls) == 1 and calls[0][-1] == CID and cid.exists()


@pytest.mark.parametrize("kind", ["foreign-live", "unknown-error"])
def test_offline_cleanup_rejects_unknown_without_stop(tmp_path, monkeypatch, kind):
    cid = tmp_path / "runtime/real/runs/synthetic-01/data/behavior.cid"
    cid.parent.mkdir(parents=True)
    cid.write_text(CID)
    calls = []
    def run(command, **kwargs):
        calls.append(command)
        return SimpleNamespace(returncode=0, stdout=b"other /snow-spark-yarn", stderr=b"") if kind == "foreign-live" else (
            SimpleNamespace(returncode=1, stdout=b"", stderr=b"Cannot connect to the Docker daemon"))
    monkeypatch.setattr(small.subprocess, "run", run)
    with pytest.raises((ValueError, RuntimeError)):
        small.driver_cleanup(tmp_path, "behavior", "synthetic-01")
    assert len(calls) == 1 and cid.exists()


def hive_reservation(tmp_path):
    # The existing envelope validator and source manifest checks remain in use.
    from test_real_hive_dispatch import envelope, source_root
    root = source_root(tmp_path)
    request = envelope(root)
    name = "snow-real-hive-" + request["request_sha256"][:20]
    write_json(hive._worker_record(root, request), dict(request=request, name=name))
    cid = hive._controlled_path(root, f"runtime/real/lifecycle/prod01/hive-requests/{request['request_sha256']}.cid")
    cid.write_text(CID)
    return root, request, name, cid


@pytest.mark.parametrize("was_running", [False, True])
def test_hive_exact_name_missing_initial_or_after_stop(tmp_path, was_running):
    root, request, name, cid = hive_reservation(tmp_path)
    calls = []
    def run(command, **kwargs):
        calls.append(command)
        if was_running and len(calls) == 1:
            return SimpleNamespace(returncode=0, stdout=f"{CID}|/{name}|true|{request['request_sha256']}\n".encode(), stderr=b"")
        if "stop" in command:
            assert command[-1] == CID
            return SimpleNamespace(returncode=0)
        return missing(name, formatted=True)
    hive.stop_worker(root, request, run=run)
    assert len(calls) == (3 if was_running else 1) and cid.exists()


@pytest.mark.parametrize("stage", ["initial", "readback"])
def test_hive_wrong_name_absence_remains_failure(tmp_path, stage):
    root, request, name, cid = hive_reservation(tmp_path)
    calls = []
    def run(command, **kwargs):
        calls.append(command)
        if stage == "readback" and len(calls) == 1:
            return SimpleNamespace(returncode=0, stdout=f"{CID}|/{name}|false|{request['request_sha256']}\n".encode(), stderr=b"")
        return missing("snow-real-hive-" + "f" * 20, formatted=True)
    with pytest.raises(ValueError):
        hive.stop_worker(root, request, run=run)
    assert len(calls) == (2 if stage == "readback" else 1) and cid.exists()


def test_lake_missing_requires_both_name_reads_then_removes_only_cidfile(tmp_path, monkeypatch):
    cid = tmp_path / "data/driver.cid"
    cid.parent.mkdir()
    cid.write_text(CID)
    calls = []
    def run(command, **kwargs):
        calls.append(command)
        assert command == ["sudo", "docker", "inspect", LAKE]
        return missing(LAKE, formatted=False)
    monkeypatch.setattr(lake.subprocess, "run", run)
    lake._clean_driver(tmp_path, tmp_path, LAKE)
    assert len(calls) == 2 and not cid.exists()


@pytest.mark.parametrize("failure", ["initial", "readback"])
def test_lake_unknown_absence_preserves_registered_cid(tmp_path, monkeypatch, failure):
    cid = tmp_path / "data/driver.cid"
    cid.parent.mkdir()
    cid.write_text(CID)
    calls = []
    def run(command, **kwargs):
        calls.append(command)
        if failure == "readback" and len(calls) == 1:
            return missing(LAKE, formatted=False)
        return subprocess.CompletedProcess(command, 1, b"[]\n", b"error: no such object: unknown\n")
    monkeypatch.setattr(lake.subprocess, "run", run)
    with pytest.raises(RuntimeError):
        lake._clean_driver(tmp_path, tmp_path, LAKE)
    assert len(calls) == (2 if failure == "readback" else 1) and cid.exists()


def test_hive_engine_manifest_explicitly_binds_new_helper(tmp_path):
    from test_real_hive_dispatch import envelope, source_root
    root = source_root(tmp_path)
    request = envelope(root)
    assert "src/snow_statistics/docker_absence.py" in request["engine"]
    request["engine"].pop("src/snow_statistics/docker_absence.py")
    with pytest.raises(ValueError):
        hive.validate_envelope(request)


def test_changed_helper_bytes_reject_before_any_hive_launch(tmp_path):
    from test_real_hive_dispatch import envelope, source_root
    root = source_root(tmp_path)
    request = envelope(root)
    (root / "src/snow_statistics/docker_absence.py").write_text("changed synthetic helper")
    def forbidden(*args, **kwargs):
        pytest.fail("Changed source cannot launch or stop an unreserved driver")
    with pytest.raises(ValueError, match="sources differ"):
        hive.run_control(request, root, runner=forbidden, stop=forbidden)
