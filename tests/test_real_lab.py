import copy
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_real_ods import COLLECTOR
from test_real_remote_lifecycle import setup

from snow_statistics.io import digest, write_json
from snow_statistics.publication import canonical
from snow_statistics.real_lab import (
    NODES,
    PHASES,
    Runner,
    compose,
    lifecycle_phase,
    metadata_paths,
    route,
    stage_nodes,
    stop_driver,
    validate_config,
    validate_job,
    validate_transfer,
)

ROOT = Path(__file__).resolve().parents[1]


def config():
    return json.loads((ROOT / "deploy/real-run.example.json").read_bytes())


def test_explicit_phases_and_low_memory_services_do_not_add_business_or_synthetic_dependencies():
    value = validate_config(config())
    assert "all" not in PHASES
    assert route(value, "sync") == "snow-analysis"
    assert route(value, "daily") == "snow-control"
    assert route(value, "permit") == "snow-analysis"
    assert all(route(value, phase) == "snow-analysis" for phase in ("initialize-writer", "submit-writer", "writer-status", "pause-writer", "resume-writer"))
    for node in NODES:
        start = compose(node, "up")
        assert "--no-build" in start and "--no-deps" in start
        assert not {"mysql", "connect", "doris-fe", "doris-be", "flink-jobmanager"} & set(start)
        stop = compose(node, "stop")
        assert "stop" in stop and not {"down", "-v", "prune", "rm"} & set(stop)
    assert "lab/compose.control-scale.yaml" in compose("snow-control", "up")
    assert "lab/compose.compute-scale.yaml" in compose("snow-compute", "up")


@pytest.mark.parametrize("mutation", [
    lambda v: v.update(command="arbitrary shell"),
    lambda v: v.update(source="synthetic"),
    lambda v: v.update(schema_version=True),
    lambda v: v.update(lane="../../business"),
    lambda v: v.update(transport_node="production"),
    lambda v: v.update(collector_url="https://public-host.example/"),
    lambda v: v.update(reader_token_file="../private.key"),
    lambda v: v["tunnel"].update(host="server;whoami"),
    lambda v: v["tunnel"].update(remote_port=True),
    lambda v: v["nodes"].update({"snow-compute": "8.8.8.8"}),
    lambda v: v.update(transport_node="snow-control"),
])
def test_configuration_is_not_an_arbitrary_command_or_filesystem_channel(mutation):
    value = config()
    mutation(value)
    with pytest.raises((ValueError, TypeError)):
        validate_config(value)


def test_boot_failure_soft_stops_only_the_vms_started_by_this_attempt(tmp_path):
    class FixtureRunner(Runner):
        def __init__(self):
            super().__init__(config(), "runtime/real/config/test.json", tmp_path)
            self.calls = []

        def run(self, command, **kwargs):
            self.calls.append(command)
            if command[2] == "start" and command[4] == "snow-compute":
                raise RuntimeError("injected boot failure")

    runner = FixtureRunner()
    with pytest.raises(RuntimeError):
        runner.windows_start("offline")
    assert runner.calls[-1][2:] == ["stop", "--node", "snow-control"]
    assert not any("snow-analysis" in call for call in runner.calls)
    assert not any("hard" in call or "delete" in call for call in runner.calls)


def test_real_storage_boot_uses_one_vm_and_preserves_offline_nodes(tmp_path):
    class FixtureRunner(Runner):
        def __init__(self):
            super().__init__(config(), "runtime/real/config/test.json", tmp_path)
            self.calls = []

        def run(self, command, **kwargs):
            self.calls.append(command)

        def remote(self, node, phase, run_id=None):
            self.calls.append([node, phase])

    runner = FixtureRunner()
    runner.windows_start("storage")
    assert all("snow-control" not in call and "snow-compute" not in call for call in runner.calls)
    assert runner.calls[-1] == ["snow-analysis", "start-storage"]
    assert any("realtime" in call for call in runner.calls)
    assert stage_nodes(config(), "offline") == NODES
    assert stage_nodes(config() | {"input_origin": "synthetic fixtures"}, "storage") == ("snow-control", "snow-analysis")
    with pytest.raises(ValueError, match="Unknown VM"):
        stage_nodes(config(), "all")


def test_model_job_rejects_arbitrary_paths_hive_and_unregistered_auxiliary():
    value = config()
    run = "test001"
    prefix = "hdfs://" + value["nodes"]["snow-control"] + ":9000/snow/"
    job = dict(run_id=run, kind="daily", source="real", input=prefix + "ods/real/kafka/" + value["lane"] + "/snapshots/" + "a" * 64 + "/_snapshot.json",
               warehouse_root=prefix + "warehouse/real/" + value["lane"], auxiliary_root=prefix + "auxiliary/real/" + value["lane"],
               date_from="2026-09-01", date_to="2026-09-02", cutoff="2026-09-03T00:00:00Z", coverage_file=run + ".json",
               auxiliary_file=None, permit_file=run + ".json", register_hive=False)
    assert validate_job(job, value, run)
    for change in ({"input": "hdfs://private/other"}, {"register_hive": True}, {"auxiliary_file": "unregistered.json"},
                   {"run_id": "other"}, {"shell": "echo private"}):
        with pytest.raises(ValueError):
            validate_job(job | change, value, run)
    with pytest.raises(ValueError):
        metadata_paths(value, "../private")


def test_missing_input_does_not_write_fake_zero_reports_or_issue_a_permit(tmp_path):
    result = lifecycle_phase(config(), tmp_path, "permit", "test")
    assert result["status"] == "no_computable_input"
    assert not {"daily", "metrics", "permit", "pv"} & set(result)
    assert not list(tmp_path.rglob("*.json"))


def test_prepare_marks_captured_kafka_initialized_before_any_compute_permission(tmp_path):
    value = config()
    value.update(input_origin="synthetic fixtures", lane="fixture-runner", transport_node="snow-control", tunnel=None)
    now = datetime.now(UTC)
    prefix = "hdfs://" + value["nodes"]["snow-control"] + ":9000/snow/"
    topic = "snow.real." + value["lane"].replace("-", "_") + ".events.v1"
    snapshot = dict(schema_version=2, source="real", identity=dict(collector=COLLECTOR, topic_ids={topic: "fixture-id"},
                    cluster_id="fixture-cluster", group="snow-ods-" + value["lane"]), offsets={topic + ":0": 1},
                    batches=[dict(batch_id="a" * 64, files={}, counts={"events": 1, "quarantine": 0},
                                  original_min_accepted_at=(now - timedelta(days=1)).isoformat(),
                                  expires_at=(now + timedelta(days=6)).isoformat())],
                    root=prefix + "ods/real/kafka/" + value["lane"], head_batch_id="a" * 64)
    token = digest(canonical(snapshot))
    state = snapshot | dict(snapshot_id=token, batch_id="a" * 64, input=snapshot["root"] + "/snapshots/" + token + "/_snapshot.json")
    write_json(tmp_path / "runtime/real/ods" / value["lane"] / "state.json", state)
    day = (now - timedelta(days=2)).date().isoformat()
    job = dict(run_id="new_job", kind="daily", source="real", input=state["input"],
               warehouse_root=prefix + "warehouse/real/" + value["lane"], auxiliary_root=prefix + "auxiliary/real/" + value["lane"],
               date_from=day, date_to=day, cutoff=now.isoformat(), coverage_file="new_job.json", auxiliary_file=None,
               permit_file="new_job.json", register_hive=False)
    write_json(tmp_path / "runtime/real/jobs/new_job.json", job)
    result = lifecycle_phase(value, tmp_path, "prepare", "new_job")
    assert result["read_permission"] is False
    registry = json.loads((tmp_path / "runtime/real/lifecycle" / value["lane"] / "registry.json").read_bytes())
    assert topic in registry["backends"]["kafka"]["resources"]
    assert registry["backends"]["kafka"]["state"] != "not_initialized"


def test_stopped_initialized_backend_does_not_become_a_passed_flag(tmp_path):
    manager, job, coverage, hdfs, ods, sink = setup(tmp_path)
    now = datetime(2026, 9, 14, tzinfo=UTC)
    manager.reserve_job(job, (now - timedelta(days=1)).isoformat(), (now - timedelta(days=1)).isoformat(), now=now)
    manager.register_backend("kafka", "snow.real.fixture.events.v1", "raw", (now - timedelta(days=1)).isoformat(), now=now)
    with pytest.raises(ValueError, match="Missing actual"):
        manager.issue_permit(job, coverage, None, tmp_path / "permit.json", hdfs, ods, sink, now=now)
    assert manager.journal.exists() and not (tmp_path / "permit.json").exists()


def test_actual_writer_phase_is_scoped_and_failure_stops_only_its_epoch(tmp_path, monkeypatch):
    from contextlib import nullcontext

    from snow_statistics.real_lab import execute_linux
    calls = []
    value = config()
    token = tmp_path / value["reader_token_file"]
    token.parent.mkdir(parents=True)
    token.write_text("fixture-reader-token-not-a-real-secret")
    token.chmod(0o600)
    monkeypatch.setattr("snow_statistics.real_lab.REMOTE_ROOT", tmp_path.as_posix())
    monkeypatch.setattr("snow_statistics.real_lab.socket.gethostname", lambda: "snow-analysis")
    monkeypatch.setattr("snow_statistics.real_lab.tunnel", lambda *_: nullcontext())
    class FixtureWriter:
        def __init__(self, root, lane, url, token):
            calls.append(("create", root, lane))
            self.epoch = SimpleNamespace(stop=lambda: calls.append(("stop", lane)))
        def initialize(self):
            raise RuntimeError("injected real API error")
    monkeypatch.setattr("snow_statistics.real_writer.ActualWriter", FixtureWriter)
    with pytest.raises(RuntimeError):
        execute_linux(Runner(value, "runtime/real/config/test.json", tmp_path), "initialize-writer")
    assert calls[-1] == ("stop", value["lane"])
    assert calls[0][1] == tmp_path / "runtime/real/epochs"
    value.update(input_origin="synthetic fixtures", lane="fixture-one")
    with pytest.raises(ValueError, match="cannot promote"):
        execute_linux(Runner(value, "runtime/real/config/test.json", tmp_path), "initialize-writer")


def test_doris_publish_uses_only_analysis_cli_and_stops_its_epoch_on_failure(tmp_path, monkeypatch):
    from contextlib import nullcontext

    from snow_statistics.real_lab import execute_linux
    value = config()
    token = tmp_path / value["reader_token_file"]
    token.parent.mkdir(parents=True)
    token.write_text("synthetic-token-file")
    token.chmod(0o600)
    calls = []
    monkeypatch.setattr("snow_statistics.real_lab.REMOTE_ROOT", tmp_path.as_posix())
    monkeypatch.setattr("snow_statistics.real_lab.socket.gethostname", lambda: "snow-analysis")
    monkeypatch.setattr("snow_statistics.real_lab.tunnel", lambda *_: nullcontext())
    monkeypatch.setattr("snow_statistics.real_quiescent.DockerStorage", lambda: None)
    monkeypatch.setattr("snow_statistics.real_epoch.Epoch", lambda folder, docker: SimpleNamespace(stop=lambda: calls.append(("stop", folder))))
    monkeypatch.setattr("snow_statistics.real_publication.read_real_release", lambda directory: {"run_id": "run01"})
    class FixtureRunner(Runner):
        def run(self, command, **kwargs):
            calls.append(("cli", command))
            raise RuntimeError("injected writer CLI failure")
    runner = FixtureRunner(value, "runtime/real/config/test.json", tmp_path)
    with pytest.raises(RuntimeError):
        execute_linux(runner, "publish-doris", "run01")
    command = calls[0][1]
    assert command[1] == "tools/real_writer.py" and command[-1] == "publish"
    assert command[command.index("--release-directory") + 1] == str(tmp_path / "runtime/real/transfers" / value["lane"] / "run01/published")
    assert "synthetic-token-file" not in command
    assert calls[-1] == ("stop", tmp_path / "runtime/real/epochs" / value["lane"])
    monkeypatch.setattr("snow_statistics.real_lab.socket.gethostname", lambda: "snow-control")
    with pytest.raises(ValueError, match="different explicitly"):
        execute_linux(runner, "publish-doris", "run01")


@pytest.mark.parametrize("phase", ["pause-writer", "resume-writer"])
def test_recovery_lock_contention_is_not_followed_by_runner_double_stop(tmp_path, monkeypatch, phase):
    from contextlib import nullcontext

    from snow_statistics.real_lab import execute_linux
    calls = []
    value = config()
    token = tmp_path / value["reader_token_file"]
    token.parent.mkdir(parents=True)
    token.write_text("synthetic-token-file")
    token.chmod(0o600)
    monkeypatch.setattr("snow_statistics.real_lab.REMOTE_ROOT", tmp_path.as_posix())
    monkeypatch.setattr("snow_statistics.real_lab.socket.gethostname", lambda: "snow-analysis")
    monkeypatch.setattr("snow_statistics.real_lab.tunnel", lambda *_: nullcontext())
    writer = SimpleNamespace(epoch=SimpleNamespace(stop=lambda: calls.append("unsafe second stop")))
    monkeypatch.setattr("snow_statistics.real_writer.ActualWriter", lambda *args: writer)
    def busy():
        raise BlockingIOError("producer batch still running")
    monkeypatch.setattr("snow_statistics.real_writer_recovery.WriterRecovery", lambda actual: SimpleNamespace(pause=busy, resume=busy))
    with pytest.raises(BlockingIOError):
        execute_linux(Runner(value, "runtime/real/config/test.json", tmp_path), phase)
    assert calls == []


def test_offline_start_refuses_to_discard_an_unpaused_real_writer(tmp_path, monkeypatch):
    from test_real_quiescent import candidate

    from snow_statistics.real_lab import execute_linux
    base = tmp_path / "runtime/real"
    base.mkdir(parents=True)
    registry, docker, _ = candidate(base)
    calls = []
    monkeypatch.setattr("snow_statistics.real_lab.REMOTE_ROOT", tmp_path.as_posix())
    monkeypatch.setattr("snow_statistics.real_lab.socket.gethostname", lambda: "snow-analysis")
    monkeypatch.setattr("snow_statistics.real_epoch.DockerEpoch", lambda: docker)
    runner = Runner(config(), "runtime/real/config/test.json", tmp_path)
    monkeypatch.setattr(runner, "run", lambda command, **kwargs: calls.append(command))
    with pytest.raises(ValueError, match="Pause the registered"):
        execute_linux(runner, "node-start-offline")
    assert not calls and all(item["running"] for item in docker.c.values())


def test_cleanup_stops_only_the_container_id_created_by_this_job(tmp_path, monkeypatch):
    cid = tmp_path / "daily.cid"
    cid.write_text("a" * 64)
    calls = []
    runner = SimpleNamespace(run=lambda command, **kwargs: calls.append(command))
    monkeypatch.setattr("snow_statistics.real_lab.subprocess.run", lambda command, **kwargs: SimpleNamespace(stdout="a" * 64 + "\n"))
    stop_driver(runner, cid)
    assert calls == [["sudo", "docker", "stop", "-t", "10", "a" * 64]]
    calls.clear()
    monkeypatch.setattr("snow_statistics.real_lab.subprocess.run", lambda command, **kwargs: SimpleNamespace(stdout=""))
    stop_driver(runner, cid)
    assert calls == []
    cid.write_text("snow-spark-yarn; arbitrary")
    with pytest.raises(ValueError, match="identity"):
        stop_driver(runner, cid)


def test_metadata_transfer_checks_exact_job_and_source_without_copying_raw_rows(tmp_path):
    manager, job, coverage_path, hdfs, ods, sink = setup(tmp_path)
    now = datetime.now(UTC)
    fixture = config()
    fixture.update(lane="lifecycle-fixture")
    # Synthetic metadata is rebuilt to the runner's precise root conventions.
    node = fixture["nodes"]["snow-control"]
    old_root = "hdfs://snow-control:9000/snow/"
    new_root = "hdfs://" + node + ":9000/snow/"
    job.update(kind="daily", register_hive=False, auxiliary_file=None, coverage_file="new_job.json", permit_file="new_job.json",
               warehouse_root=new_root + "warehouse/real/lifecycle-fixture", auxiliary_root=new_root + "auxiliary/real/lifecycle-fixture")
    state = json.loads((ods / "state.json").read_bytes())
    from snow_statistics.io import digest
    from snow_statistics.publication import canonical
    snapshot = {key: state[key] for key in ("schema_version", "source", "identity", "offsets", "batches", "root", "head_batch_id")}
    snapshot["root"] = snapshot["root"].replace(old_root, new_root)
    token = digest(canonical(snapshot))
    state = snapshot | dict(snapshot_id=token, batch_id=snapshot["head_batch_id"], input=snapshot["root"] + "/snapshots/" + token + "/_snapshot.json")
    job["input"] = state["input"]
    coverage = json.loads(coverage_path.read_bytes())
    from snow_statistics.real_remote_lifecycle import outputs_for
    permit = dict(schema_version=1, source="real", input_snapshot=job["input"], coverage_sha256="a" * 64, auxiliary_sha256=None,
                  outputs=outputs_for(job), hive_tables=[], issued_at=now.isoformat(), expires_at=(now + timedelta(minutes=10)).isoformat(),
                  lifecycle_receipt_sha256="b" * 64)
    receipt = dict(owner={key: coverage[key] for key in ("instance_id", "generation")})
    values = dict(job=job, coverage=coverage, permit=permit, receipt=receipt, state=state)
    assert validate_transfer(values, fixture, "new_job")
    changed = copy.deepcopy(values)
    changed["state"]["raw_events"] = [{"private": "never transfer"}]
    with pytest.raises(ValueError, match="metadata fields"):
        validate_transfer(changed, fixture, "new_job")
    changed = copy.deepcopy(values)
    changed["permit"]["expires_at"] = (now - timedelta(seconds=1)).isoformat()
    with pytest.raises(ValueError, match="live exact job"):
        validate_transfer(changed, fixture, "new_job")
