import importlib.util
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from snow_statistics.config import Settings
from snow_statistics.contracts import Event
from snow_statistics.publication import publication_database
from snow_statistics.real_engine_fixture import (
    be_integer_settings,
    diagnostics,
    events,
    java_environment,
    oracle,
    restored_checkpoint,
    retained_checkpoint,
    schema_statements,
    test_scope,
    verify_be_setting,
)
from snow_statistics.real_epoch import compose_spec, make_manifest
from snow_statistics.store import Store

NOW = datetime(2026, 9, 13, 8, tzinfo=UTC)
IMAGES = {key: "fixture@sha256:" + "a" * 64 for key in ("KAFKA_IMAGE", "DORIS_FE_IMAGE", "DORIS_BE_IMAGE", "FLINK_IMAGE")}


def manifest():
    return make_manifest("fixture-engine-01", NOW.isoformat(), IMAGES, synthetic_engine_test=True, now=NOW)


def test_synthetic_engine_mode_never_masquerades_as_real_user_epoch(tmp_path):
    value = manifest()
    scope = test_scope(value)
    assert value["source"] == "real" and value["input_origin"] == "synthetic fixtures"
    assert scope["lane"] == "fixture_engine_01"
    jar = tmp_path / "test.jar"
    jar.write_bytes(b"synthetic jar marker")
    spec = compose_spec(value, tmp_path, "192.168.65.3", jar)
    assert len(spec["services"]) == 5
    assert spec["services"]["doris-be"]["pids_limit"] == 512
    assert all(value["pids_limit"] == 256 for role, value in spec["services"].items() if role != "doris-be")
    for role in ("jobmanager", "taskmanager"):
        env = spec["services"][role]["environment"]
        assert env["SNOW_REPLAY_LANE"] == scope["lane"]
        assert env["SNOW_INPUT_TOPIC"] == scope["topics"]["events"]
    real = make_manifest("epoch-real", NOW.isoformat(), IMAGES, now=NOW)
    with pytest.raises(ValueError, match="explicit"):
        test_scope(real)


def test_twenty_recent_events_have_hand_checked_lite_and_oracle_integer_metrics(tmp_path):
    generated = events("fixture_engine_01", NOW, 20)
    store = Store(Settings(mode="full", source="real", db=tmp_path / "statistics.db",
                           allowed_characters=frozenset({"sample_character"})), clock=lambda: NOW)
    try:
        assert store.ingest([Event.model_validate(row) for row in generated])["accepted"] == 20
        while store.aggregate():
            pass
        rows = store.read()["events"]
        expected = [dict(source="real", app="mywebsite", date="2026-09-13", pv=5, uv=3, requests=0, successes=0),
                    dict(source="real", app="project_snow", date="2026-09-13", pv=5, uv=2, requests=5, successes=3)]
        assert oracle(rows) == expected
        assert [dict(row) for row in store.db.execute("SELECT source,app,day AS date,pv,uv,requests,successes FROM daily ORDER BY source,day,app")] == expected
        negative = diagnostics(rows, NOW)
        assert negative["wrong_source"]["source"] == "synthetic"
        assert all(row["source"] == "real" for row in rows)
        assert negative["request_duplicate"]["event"]["request_id"] == "fixture_req_0"
        assert negative["conflict"]["event"]["event_id"] == rows[0]["event"]["event_id"]
    finally:
        store.close()


def test_runtime_job_environment_binds_same_epoch_topic_window_and_zero_offset():
    value = manifest()
    scope = test_scope(value)
    identity = dict(cluster_id="cluster-fixture", topic_ids={scope["topics"]["events"]: "topic-fixture"})
    env = java_environment(value, scope, identity, "a" * 32, "192.168.65.3")
    assert env["SNOW_SOURCE"] == "real" and env["SNOW_REAL_START_OFFSET"] == "0"
    assert env["SNOW_REAL_READABLE_FROM"] == value["original_min_accepted_at"]
    assert env["SNOW_REAL_RESTORE_NOT_AFTER"] == value["expires_at"]
    assert env["DORIS_TABLE"].startswith(scope["database"] + ".")
    assert not any(key.startswith("SNOW_REAL_EPOCH_") for key in env)
    with pytest.raises(ValueError, match="identity"):
        java_environment(value, scope, identity | {"cluster_id": "bad\nOTHER=1"}, "a" * 32, "192.168.65.3")


def test_real_publication_and_all_schema_objects_use_only_new_fixture_database(monkeypatch):
    database = test_scope(manifest())["database"]
    monkeypatch.setenv("SNOW_DORIS_DATABASE", database)
    assert publication_database("real") == database
    root = Path(__file__).resolve().parents[1]
    for filename in ("schema.sql", "publication.sql"):
        rendered = "\n".join(schema_statements((root / "warehouse/doris" / filename).read_text(), database))
        assert "snow." not in rendered and "EXISTS snow;" not in rendered
        assert database + "." in rendered
    with pytest.raises(ValueError):
        schema_statements("CREATE DATABASE snow;", "snow_realtime_old")


@pytest.mark.parametrize("count", [0, 9, 101, 1000])
def test_fixture_count_is_bounded(count):
    with pytest.raises(ValueError):
        events("fixture_engine_01", NOW, count)


def test_isolated_loopback_collector_accepts_bounded_fixture_with_trusted_completion(tmp_path):
    path = Path(__file__).resolve().parents[1] / "tools/smoke_real_epoch.py"
    spec = importlib.util.spec_from_file_location("smoke_real_epoch_fixture", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    generated = events("fixture_engine_01", datetime.now(UTC), 10)
    with module.local_collector(tmp_path) as (client, store):
        assert client.base_url.host == "127.0.0.1"
        response = client.post("/analytics/v1/events", json={"events": generated})
        assert response.status_code == 202 and response.json()["accepted"] == 10
        rows = store.read()["events"]
        assert len(rows) == 10 and all(row["source"] == "real" for row in rows)
        assert client.post("/analytics/v1/events", json={"events": generated}).json()["accepted"] == 0
        assert len(store.read()["events"]) == 10


def test_checkpoint_requires_exact_completed_state_and_accepts_rest_api_restore_semantics():
    job = "a" * 32
    previous = dict(status="COMPLETED", discarded=False, is_savepoint=False, id=7,
                    num_subtasks=5, num_acknowledged_subtasks=5, external_path=f"file:/checkpoints/{job}/chk-7")
    assert retained_checkpoint(previous, job) == previous["external_path"]
    actual = dict(counts=dict(restored=1), latest=dict(restored=dict(id=7, is_savepoint=True,
                  external_path=f"file:///checkpoints/{job}/chk-7")))
    assert restored_checkpoint(actual, previous, job)["is_savepoint"] is True
    for key, wrong in {"discarded": True, "is_savepoint": True, "num_acknowledged_subtasks": 4,
                       "external_path": f"file:/checkpoints/{'b' * 32}/chk-7", "id": True}.items():
        with pytest.raises(ValueError):
            retained_checkpoint(previous | {key: wrong}, job)
    actual["latest"]["restored"]["id"] = 8
    with pytest.raises(ValueError):
        restored_checkpoint(actual, previous, job)


def test_session_resume_starts_owned_storage_then_uses_exact_checkpoint_and_no_claim(tmp_path, monkeypatch):
    import sys

    import httpx
    path = Path(__file__).resolve().parents[1] / "tools/smoke_real_epoch.py"
    spec = importlib.util.spec_from_file_location("smoke_real_epoch_resume", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    task = module.Acceptance.__new__(module.Acceptance)
    task.manifest = manifest() | dict(jar=dict(sha256="a" * 64))
    task.directory, task.scope, task.host = tmp_path, test_scope(task.manifest), "192.168.65.3"
    task.database = task.scope["database"]
    rows = [dict(seq=index + 1, source="real", accepted_at=NOW.isoformat(), event=event)
            for index, event in enumerate(events(task.scope["lane"], NOW, 20))]
    expected = oracle(rows)
    old, new = "b" * 32, "c" * 32
    checkpoint = dict(status="COMPLETED", discarded=False, is_savepoint=False, id=7,
                      num_subtasks=5, num_acknowledged_subtasks=5, external_path=f"file:/checkpoints/{old}/chk-7")
    files = dict(files={f"/checkpoints/{old}/chk-7/_metadata": dict(bytes=1, sha256="d" * 64)}, total_bytes=1, sha256="e" * 64)
    previous = {key: task.manifest[key] for key in ("epoch_id", "generation", "original_min_accepted_at", "expires_at")}
    previous.update(runtime_jar_sha256="a" * 64, previous_job_id=old, checkpoint=checkpoint, checkpoint_files=files)
    for name, value in {"session-pause.json": previous, "fixture-input.json": dict(on_time=rows),
                        "acceptance.json": dict(session_recovery_pending=True, integer_metrics=expected)}.items():
        (tmp_path / name).write_text(json.dumps(value))
    calls, sent = [], []
    task.epoch = SimpleNamespace(start=lambda stage: calls.append(stage), stop=lambda: calls.append("stop"))
    task.current = lambda: ({"owned": dict(running=False)}, {})
    task.checkpoint_files = lambda: files
    task.be_readback = lambda phase: dict(phase=phase, unit_test_only=True)
    task.query = lambda _: [(20,)]
    task.metrics = lambda: expected
    task.side = lambda _: [None] * (3 + len(sent))
    def command(arguments, **kwargs):
        calls.append(arguments)
        return ("JobID " + new).encode() if "run" in arguments else ("a" * 64 + "  runtime.jar").encode()
    task.docker = SimpleNamespace(command=command)
    restored = dict(counts=dict(restored=1), latest=dict(restored=dict(id=7, is_savepoint=True,
                                                                          external_path=checkpoint["external_path"])))
    class Flink:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def get(self, url):
            value = dict(taskmanagers=1) if url == "/overview" else dict(jobs=[]) if url == "/jobs/overview" else restored if url.endswith("/checkpoints") else dict(state="RUNNING")
            return SimpleNamespace(json=lambda: value, raise_for_status=lambda: None)

        def patch(self, *args, **kwargs):
            return SimpleNamespace(raise_for_status=lambda: None)
    monkeypatch.setattr(httpx, "Client", lambda **_: Flink())
    def send(topic, **kwargs):
        sent.append(kwargs)
        return SimpleNamespace(get=lambda **_: None)
    monkeypatch.setitem(sys.modules, "kafka", SimpleNamespace(
        KafkaProducer=lambda **_: SimpleNamespace(send=send, close=lambda **_: None)))
    result = task.resume()
    submission = next(value for value in calls if isinstance(value, list) and "run" in value)
    assert calls[:2] == ["storage", "realtime"] and calls[-1] == "stop"
    assert submission[submission.index("--fromSavepoint") + 1] == checkpoint["external_path"]
    assert submission[submission.index("--claimMode") + 1] == "no_claim"
    assert "-n" not in submission and "--allowNonRestoredState" not in submission
    assert result["duplicate_count_after"] == result["duplicate_count_before"] + 2
    assert result["integer_metrics"] == expected and result["new_job_id"] != old
    assert json.loads((tmp_path / "acceptance.json").read_bytes())["full_session_checkpoint_restore_verified"] is True


def test_all_three_eager_brpc_pools_are_bounded_even_with_flight_listener_disabled():
    path = Path(__file__).resolve().parents[1] / "deploy/real-epoch/be.conf"
    text = path.read_text()
    values = be_integer_settings(text)
    assert values["arrow_flight_sql_port"] == -1
    pools = ("brpc_heavy_work_pool_threads", "brpc_light_work_pool_threads", "brpc_arrow_flight_work_pool_threads")
    assert sum(values[key] for key in pools) == 12
    for key in pools:
        with pytest.raises(ValueError, match="three eager"):
            be_integer_settings(text.replace(key + " = 4", key + " = -1"))
    with pytest.raises(ValueError, match="duplicate"):
        be_integer_settings(text + "\nbrpc_heavy_work_pool_threads = 4\n")


@pytest.mark.parametrize("rows", [[], [["wrong_option", "int32_t", "4"]],
                                  [["brpc_heavy_work_pool_threads", "int32_t", "-1"]],
                                  [["brpc_heavy_work_pool_threads", "int32_t", True]],
                                  [["brpc_heavy_work_pool_threads", "int32_t", "4"]] * 2])
def test_runtime_be_readback_rejects_missing_wrong_or_ambiguous_setting(rows):
    with pytest.raises(ValueError, match="Actual BE"):
        verify_be_setting("brpc_heavy_work_pool_threads", 4, rows)


def test_runtime_be_readback_requires_exact_option_and_actual_value():
    assert verify_be_setting("brpc_arrow_flight_work_pool_threads", 4,
                             [["brpc_arrow_flight_work_pool_threads", "int32_t", "4", "false"]])["actual_api_read"] is True


def test_fixture_account_uses_doris_identifier_role_and_parameterized_percent_host():
    from snow_statistics.real_engine_fixture import account_statements
    scope = test_scope(manifest())
    statements = account_statements(scope, dict(user=scope["user"], password="synthetic_password"))
    assert statements[0] == (f"CREATE ROLE `{scope['role']}`", ())
    assert statements[1][0].endswith("TO ROLE '" + scope["role"] + "'")
    sql, parameters = statements[2]
    assert parameters == ("%", "synthetic_password")
    rendered = sql % tuple(repr(value) for value in parameters)
    assert "@'%' IDENTIFIED BY 'synthetic_password' DEFAULT ROLE '" in rendered
    with pytest.raises(ValueError, match="isolated namespace"):
        account_statements(scope | dict(database="snow"), dict(user=scope["user"], password="unused"))


@pytest.mark.parametrize("unexpected", [None, "database", "table", "data", "role", "user", "topic", "column", "width",
                                         "granted_ok", "granted_excessive", "granted_evidence_changed"])
def test_empty_bootstrap_retry_checks_every_live_namespace(tmp_path, unexpected):
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("smoke_retry", root / "tools/smoke_real_epoch.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    task = module.Acceptance.__new__(module.Acceptance)
    task.manifest, task.directory = manifest(), tmp_path
    task.scope = test_scope(task.manifest)
    task.database = task.scope["database"]
    tables = [(name, "BASE TABLE") for name in ("events_realtime", "daily_offline", "daily_snapshots", "offline_releases")]
    tables += [(name, "VIEW") for name in ("daily_realtime", "daily_published", "report_published")]
    granted = bool(unexpected and unexpected.startswith("granted_"))
    role_row = dict(Name=task.scope["role"], Comment="", Users="", GlobalPrivs=None, CatalogPrivs=None,
                    DatabasePrivs="internal." + task.database + ".*: Select_priv,Load_priv", TablePrivs=None, ResourcePrivs=None,
                    CloudClusterPrivs=None, CloudStagePrivs=None, StorageVaultPrivs=None, WorkloadGroupPrivs=None, ComputeGroupPrivs=None)
    if granted:
        task.retry_role = dict(readback=dict(role=[role_row.copy()], role_names=["admin", "operator", task.scope["role"]],
                                            users=["'root'@'%'"], production_touched=False))
        if unexpected == "granted_evidence_changed":
            task.retry_role["readback"]["role"][0]["DatabasePrivs"] = "foreign"
        if unexpected == "granted_excessive":
            role_row["GlobalPrivs"] = "Admin_priv"
    def query(sql, admin):
        assert admin is True
        if sql == "SHOW DATABASES":
            return [(task.database,), ("mysql",)] + ([("foreign",)] if unexpected == "database" else [])
        if sql.startswith("SHOW FULL TABLES"):
            return tables[:-1] if unexpected == "table" else tables
        if sql.startswith("SELECT COUNT"):
            return [(1 if unexpected == "data" else 0,)]
        if sql == "SHOW ROLES":
            return [("operator",), ("admin",)] + ([(task.scope["role"],)] if unexpected == "role" or granted else [])
        if sql == "SHOW ALL GRANTS":
            return [("'root'@'%'",)] + ([("'foreign'@'%'",)] if unexpected == "user" else [])
        raise AssertionError(sql)
    task.query = query
    class Cursor:
        description = [(name,) for name in ["Tables_in_" + task.database, "Table_type", "Storage_format", "Inverted_index_storage_format"]]

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def execute(self, sql):
            self.sql = sql
            if sql == "SHOW ROLES":
                self.description = [(key,) for key in role_row]
                return
            assert sql == "SHOW FULL TABLES FROM " + task.database
            if unexpected == "column":
                self.description = [("unknown",)]

        def fetchall(self):
            if self.sql == "SHOW ROLES":
                return [tuple(role_row.values())]
            rows = query("SHOW FULL TABLES", True)
            return rows if unexpected == "width" else [(*row, *(("V2", "V2") if row[1] == "BASE TABLE" else ("NONE", "NONE"))) for row in rows]

        def cursor(self):
            return self
    task.connect = lambda admin: Cursor()
    admin = SimpleNamespace(list_topics=lambda: ["unexpected"] if unexpected == "topic" else [])
    if unexpected and unexpected != "granted_ok":
        with pytest.raises(ValueError, match="Bootstrap retry|Granted-role"):
            task.bootstrap_retry_live_checks(admin)
    else:
        result = task.bootstrap_retry_live_checks(admin)
        assert result["kafka_topics_empty"] and all(value == 0 for value in result["rows"].values())


@pytest.mark.parametrize("unexpected", [None, "digest", "checkpoint", "running", "input", "account",
                                         "granted_ok", "granted_digest", "granted_generation"])
def test_empty_bootstrap_retry_binds_failure_and_reuses_private_credentials(tmp_path, unexpected):
    import hashlib
    import os
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("smoke_retry_preflight", root / "tools/smoke_real_epoch.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    task = module.Acceptance.__new__(module.Acceptance)
    task.manifest, task.directory = manifest(), tmp_path
    task.scope = test_scope(task.manifest)
    account = dict(user="foreign" if unexpected == "account" else task.scope["user"], password="a" * 43)
    path = tmp_path / "secrets.json"
    path.write_text(json.dumps(account))
    os.chmod(path, 0o600)
    failure = json.dumps(dict(action="storage", complete=False, error_class="OperationalError", error_number=1105, owned_readers_stopped=True)).encode()
    (tmp_path / "failure.json").write_bytes(failure)
    containers = {task.manifest["containers"][name]: dict(running=unexpected == "running") for name in ("kafka", "doris-fe", "doris-be")}
    volumes = {task.manifest["volumes"][name]: {} for name in ("kafka", "doris-fe", "doris-be")}
    if unexpected == "checkpoint":
        volumes[task.manifest["volumes"]["checkpoints"]] = {}
    if unexpected == "input":
        (tmp_path / "fixture-input.json").write_text("{}")
    task.current = lambda: (containers, volumes)
    digest = "0" * 64 if unexpected == "digest" else hashlib.sha256(failure).hexdigest()
    if unexpected and unexpected.startswith("granted_"):
        archive = tmp_path / "storage-attempt-4"
        archive.mkdir()
        failure = json.dumps(dict(action="resume-bootstrap", complete=False, error_class="ValueError", error_number=None,
                                  owned_readers_stopped=True, stack=[dict(function="mogrify")])).encode()
        (archive / "failure.json").write_bytes(failure)
        (archive / "role-readback.json").write_text("{}")
        previous = {key: task.manifest[key] for key in ("epoch_id", "generation", "original_min_accepted_at", "expires_at")}
        previous.update(complete=False, input_origin="synthetic fixtures", action="explicit_empty_bootstrap_retry")
        if unexpected == "granted_generation":
            previous["generation"] = "changed"
        (tmp_path / "bootstrap-retry.json").write_text(json.dumps(previous))
        role_hash = "0" * 64 if unexpected == "granted_digest" else hashlib.sha256(b"{}").hexdigest()
        arguments = (hashlib.sha256(failure).hexdigest(), "storage-attempt-4/failure.json", role_hash)
        if unexpected == "granted_ok":
            assert task.bootstrap_retry_preflight(*arguments) == account
            assert task.retry_role["sha256"] == role_hash
        else:
            with pytest.raises(ValueError):
                task.bootstrap_retry_preflight(*arguments)
        return
    if unexpected:
        with pytest.raises(ValueError):
            task.bootstrap_retry_preflight(digest)
    else:
        assert task.bootstrap_retry_preflight(digest) == account
        assert json.loads(path.read_bytes()) == account
        archived = tmp_path / "storage-attempt-2"
        archived.mkdir()
        (archived / "failure.json").write_bytes(failure)
        (tmp_path / "failure.json").write_text("{}")
        assert task.bootstrap_retry_preflight(digest, "storage-attempt-2/failure.json") == account
        with pytest.raises(ValueError, match="exact owned"):
            task.bootstrap_retry_preflight(digest, "../failure.json")


@pytest.mark.parametrize("wrong_heap", [False, True])
def test_be_readback_uses_live_process_projection_and_exact_api_options(tmp_path, monkeypatch, wrong_heap):
    import httpx
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location("smoke_real_epoch_live_be", root / "tools/smoke_real_epoch.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    (tmp_path / "be.conf").write_text((root / "deploy/real-epoch/be.conf").read_text())
    expected = be_integer_settings((tmp_path / "be.conf").read_text())
    task = module.Acceptance.__new__(module.Acceptance)
    task.manifest, task.directory = manifest(), tmp_path
    task.epoch = SimpleNamespace(directory=tmp_path)
    task.compose = dict(services={"doris-be": dict(pids_limit=512)})
    task.current = lambda: None
    counters = {"pids.current": "363", "pids.max": "512", "pids.events": "max 0",
                "memory.current": "500000000", "memory.peak": "600000000", "memory.max": "1879048192"}
    def command(args):
        if args[0] == "inspect":
            return b'[{"State":{"Running":true,"OOMKilled":false}}]'
        if args[2] == "cat":
            return counters[args[3].rsplit("/", 1)[1]].encode()
        assert args[2:4] == ["bash", "-c"] and "grep -E '^(JAVA_OPTS|LIBHDFS_OPTS)='" in args[4]
        assert "logs" not in args
        heap = "2048" if wrong_heap else "256"
        return ("BE_PID=55\nBE_THREADS=359\nJAVA_OPTS=-Xms64m -Xmx256m\nLIBHDFS_OPTS=-Xms64m -Xmx" + heap + "m\n").encode()
    task.docker = SimpleNamespace(command=command)
    class API:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def get(self, url, params):
            assert url == "/api/show_config" and set(params) == {"conf_item"}
            key = params["conf_item"]
            return SimpleNamespace(content=b"bounded exact option", raise_for_status=lambda: None,
                                   json=lambda: [[key, "int32_t", str(expected[key]), "false"]])
    monkeypatch.setattr(httpx, "Client", lambda **_: API())
    if wrong_heap:
        with pytest.raises(AssertionError):
            task.be_readback("storage")
        assert not (tmp_path / "be-readback-storage.json").exists()
    else:
        receipt = task.be_readback("storage")
        assert set(receipt["parameters"]) == set(expected)
        assert receipt["process"]["threads"] == 359 and receipt["process"]["jni_max_heap_mib"] == 256
        assert "LIBHDFS_OPTS" not in json.dumps(receipt)
