"""Synthetic catalog/Parquet fixtures; these are not Hive engine evidence."""
import ast
import importlib.util
import json
import subprocess
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_real_publication import NOW, packages

from snow_statistics.io import digest, write_json
from snow_statistics.publication import canonical
from snow_statistics.real_hive import HiveRegistry, HiveRetention, SparkCatalog, plan, register_release
from snow_statistics.real_hive_contract import (
    GROUP_COLUMNS,
    operate,
    properties,
    rows_hash,
    validate_descriptor,
    validate_metadata,
)
from snow_statistics.real_publication import release_real
from snow_statistics.real_remote_lifecycle import RealRemoteLifecycle


def fixture(tmp_path):
    daily, behavior = packages()
    root = "hdfs://snow-control:9000/snow/warehouse/real/test"
    daily["manifest"]["output"] = root + "/runs/test"
    collector = daily["manifest"]["input_snapshot"]["collector"]
    manager = RealRemoteLifecycle(tmp_path / "metadata")
    manager.initialize(root, "hdfs://snow-control:9000/snow/auxiliary/real/test",
                       "hdfs://snow-control:9000/snow/ods/real/kafka/test", collector["instance_id"], collector["generation"])
    for path in [daily["manifest"]["output"] + "/ads_daily"] + [behavior["manifest"]["output"] + "/" + name for name in behavior["aggregates"]]:
        manager.register(path, "aggregate", "2026-09-01T00:00:00+08:00", now=NOW)
    directory = tmp_path / "publication"
    release = release_real(daily, behavior, directory, "test", now=NOW)
    registry = HiveRegistry(manager)
    return registry, release, directory


def metadata(value):
    fields = GROUP_COLUMNS[value["group"]]
    if value["group"] == "daily":
        fields = fields - {"date"} | {"business_date"}
    return dict(type="EXTERNAL", provider="parquet", location=value["location"], properties=properties(value),
                columns=sorted(fields), partition_columns=["source", "business_date"] if value["group"] == "daily" else [], partitions={})


class MemoryCatalog:
    def __init__(self, release):
        self.tables = {}
        self.values = {"daily": release["daily"]["daily"], **release["behavior"]["aggregates"]}
        self.reads, self.drops, self.creates = [], [], []
        self.retain_after_drop = False

    def names(self, prefix):
        return [name for name in self.tables if name.startswith(prefix)]

    def inspect(self, table):
        return self.tables.get(table)

    def create(self, table, value):
        self.creates.append(table)
        self.tables[table] = metadata(value)

    def drop(self, table):
        self.drops.append(table)
        if not self.retain_after_drop:
            del self.tables[table]

    def rows(self, table, value):
        self.reads.append(table)
        return self.values[value["group"]]


class MemoryRPC:
    def __init__(self, registry, release):
        self.registry, self.catalog, self.now = registry, MemoryCatalog(release), NOW

    def request(self, action, tables):
        return dict(schema_version=1, action=action, metastore_uri="thrift://192.168.5.10:9083",
                    owner_sha256=self.registry.read()["owner_sha256"], tables=tables,
                    known_tables=sorted(self.registry.read()["tables"]), requested_at=self.now.isoformat())

    def execute(self, action, tables, now):
        assert now <= self.now
        result = operate(self.request(action, tables), self.catalog, clock=lambda: self.now)
        return {"tables": result}


def cleanup(registry, rpc):
    _, data = registry.manager._read()
    return HiveRetention(registry, rpc).purge_and_verify(data["backends"]["hive"]["resources"], rpc.now)


def test_canonical_non_ascii_and_nullable_rows_match_service():
    from snow_statistics.real_hive_contract import canonical as driver_canonical
    rows = [{"channel": "主页入口", "retained_d7": None}, {"channel": "直接访问", "retained_d7": 0}]
    assert driver_canonical(rows) == canonical(rows)
    assert rows_hash(rows) == digest(canonical(sorted(rows, key=canonical)))


def test_register_verify_retry_empty_group_and_metadata_only_cleanup(tmp_path):
    registry, release, directory = fixture(tmp_path)
    rpc = MemoryRPC(registry, release)
    result = register_release(directory, registry, rpc, lambda: cleanup(registry, rpc), now=NOW)
    assert len(result["tables"]) == 4 and len(rpc.catalog.creates) == 4
    assert all(entry["verified"] for entry in registry.read()["tables"].values())
    assert any(item["rows"] == 0 for item in result["tables"].values())
    register_release(directory, registry, rpc, lambda: cleanup(registry, rpc), now=NOW)
    register_release(directory, registry, rpc, lambda: cleanup(registry, rpc), now=NOW, verify_only=True)
    assert len(rpc.catalog.creates) == 4
    before = len(rpc.catalog.reads)
    receipt = cleanup(registry, rpc)
    assert receipt["live_records"] == 4 and receipt["hdfs_payload_deleted"] is False
    assert len(rpc.catalog.reads) == before
    rpc.now = NOW + timedelta(days=90)
    assert cleanup(registry, rpc)["live_records"] == 0
    assert len(rpc.catalog.drops) == 4 and not rpc.catalog.tables
    assert cleanup(registry, rpc)["next_expiry"] is None


@pytest.mark.parametrize("change", [
    lambda owner, state, release: owner.update(generation="11111111-1111-4111-8111-111111111111"),
    lambda owner, state, release: state["artifacts"].clear(),
    lambda owner, state, release: release.update(expires_at="2027-01-01T00:00:00Z"),
    lambda owner, state, release: owner["roots"].update(warehouse="hdfs://snow-control:9000/snow/warehouse/real/other"),
    lambda owner, state, release: release["daily"]["daily"][0].update(source="synthetic"),
])
def test_plan_refuses_identity_source_location_or_lifetime_drift(tmp_path, change):
    registry, release, _ = fixture(tmp_path)
    owner, state = registry.manager._read()
    change(owner, state, release)
    with pytest.raises(ValueError):
        plan(release, owner, state["artifacts"], now=NOW)


@pytest.mark.parametrize("change", [
    lambda value: value.update(location="hdfs://snow-control:9000/snow/warehouse/synthetic/ads_daily"),
    lambda value: value.update(location="hdfs://snow-control:9000/snow/warehouse/real/test/../ads_daily"),
    lambda value: value.update(location="hdfs://snow-control:9000/snow/warehouse/real/test/'/ads_daily"),
    lambda value: value.update(expires_at="2027-01-01T00:00:00Z"),
    lambda value: value.update(expected_rows=10001),
    lambda value: value.update(expected_rows=True),
    lambda value: value.update(raw_event={"anonymous_id": "fixture-only"}),
    lambda value: value["collector"].update(source="synthetic"),
])
def test_descriptor_whitelist_rejects_unknown_and_unsafe_values(tmp_path, change):
    registry, release, _ = fixture(tmp_path)
    table, value = next(iter(registry.reserve(release, now=NOW).items()))
    change(value)
    with pytest.raises(ValueError):
        validate_descriptor(table, value)


@pytest.mark.parametrize("change", [
    lambda value: value.update(type="MANAGED"),
    lambda value: value.update(provider="hive"),
    lambda value: value.update(location="hdfs://snow-control:9000/business"),
    lambda value: value["properties"].update({"external.table.purge": "true"}),
    lambda value: value["properties"].update({"snow.source": "synthetic"}),
    lambda value: value["columns"].append("anonymous_id"),
    lambda value: value["partitions"].update({"source=synthetic/business_date=2026-09-05": "anywhere"}),
    lambda value: value["partitions"].update({"source=real/business_date=2026-09-05": "hdfs://snow-control:9000/elsewhere"}),
])
def test_actual_metadata_must_be_external_exact_and_real(tmp_path, change):
    registry, release, _ = fixture(tmp_path)
    descriptor = next(iter(registry.reserve(release, now=NOW).values()))
    actual = metadata(descriptor)
    change(actual)
    with pytest.raises(ValueError):
        validate_metadata(descriptor, actual)


def test_partial_registration_is_durable_and_retry_does_not_renew(tmp_path, monkeypatch):
    registry, release, _ = fixture(tmp_path)
    original = registry.manager.register_backend
    calls = []
    def interrupted(*args, **kwargs):
        calls.append(args)
        if len(calls) == 2:
            raise RuntimeError("synthetic interrupted reservation")
        return original(*args, **kwargs)
    monkeypatch.setattr(registry.manager, "register_backend", interrupted)
    with pytest.raises(RuntimeError):
        registry.reserve(release, now=NOW)
    assert len(registry.read()["tables"]) == 4
    rpc = MemoryRPC(registry, release)
    with pytest.raises(ValueError, match="every exact"):
        cleanup(registry, rpc)
    monkeypatch.setattr(registry.manager, "register_backend", original)
    registry.reserve(release, now=NOW + timedelta(days=1))
    assert cleanup(registry, rpc)["live_records"] == 0  # reserved, never claimed created


def test_cleanup_error_and_missing_table_block_readers(tmp_path):
    registry, release, directory = fixture(tmp_path)
    rpc = MemoryRPC(registry, release)
    def failure():
        raise RuntimeError("synthetic HDFS/backends outage")
    with pytest.raises(RuntimeError):
        register_release(directory, registry, rpc, failure, now=NOW)
    assert rpc.catalog.reads == rpc.catalog.creates == []
    register_release(directory, registry, rpc, lambda: cleanup(registry, rpc), now=NOW)
    table = next(iter(rpc.catalog.tables))
    saved = rpc.catalog.tables.pop(table)
    with pytest.raises(ValueError, match="disappeared"):
        cleanup(registry, rpc)
    rpc.catalog.tables[table] = saved
    rpc.now = NOW + timedelta(days=90)
    rpc.catalog.retain_after_drop = True
    with pytest.raises(ValueError, match="survived"):
        cleanup(registry, rpc)


def test_unregistered_owned_table_and_all_metadata_precede_reads(tmp_path):
    registry, release, _ = fixture(tmp_path)
    registry.reserve(release, now=NOW)
    rpc = MemoryRPC(registry, release)
    entries = registry.read()["tables"]
    table = next(iter(entries))
    unknown = table.replace(entries[table]["descriptor"]["release_sha256"][:32], "f" * 32)
    rpc.catalog.tables[unknown] = metadata(entries[table]["descriptor"])
    with pytest.raises(ValueError, match="Unregistered"):
        rpc.execute("register", entries, NOW)
    rpc.catalog.tables.clear()
    last = list(entries)[-1]
    rpc.catalog.tables[last] = metadata(entries[last]["descriptor"]) | {"type": "MANAGED"}
    with pytest.raises(ValueError):
        rpc.execute("register", entries, NOW)
    assert rpc.catalog.creates == rpc.catalog.reads == []


def test_exact_rows_not_only_counts_and_verify_does_not_create(tmp_path):
    registry, release, _ = fixture(tmp_path)
    registry.reserve(release, now=NOW)
    rpc = MemoryRPC(registry, release)
    entries = registry.read()["tables"]
    with pytest.raises(ValueError, match="cannot create"):
        rpc.execute("verify", entries, NOW)
    rpc.catalog.values["daily"][0]["pv"] += 1
    with pytest.raises(ValueError, match="values differ"):
        rpc.execute("register", entries, NOW)


def test_request_expiry_and_public_metastore_refused(tmp_path):
    registry, release, _ = fixture(tmp_path)
    registry.reserve(release, now=NOW)
    rpc = MemoryRPC(registry, release)
    request = rpc.request("cleanup", registry.read()["tables"])
    with pytest.raises(ValueError, match="stale"):
        operate(request, rpc.catalog, clock=lambda: NOW + timedelta(minutes=10))
    request["metastore_uri"] = "thrift://8.8.8.8:9083"
    with pytest.raises(ValueError, match="private"):
        operate(request, rpc.catalog, clock=lambda: NOW)


def test_rpc_hashes_actual_canonical_bytes_and_rejects_forged_receipts(tmp_path):
    registry, release, _ = fixture(tmp_path)
    registry.reserve(release, now=NOW)
    seen = []
    def runner(command, **kwargs):
        body = (tmp_path / command[2]).read_bytes()
        assert digest(body) == command[3] and canonical(json.loads(body)) == body
        seen.append(json.loads(body))
        return SimpleNamespace(stdout='SNOW_HIVE_RESULT={"request_sha256":"forged"}\n')
    rpc = SparkCatalog(tmp_path, registry, "192.168.5.10", runner=runner)
    with pytest.raises(ValueError, match="binding"):
        rpc.execute("cleanup", registry.read()["tables"], NOW)
    assert len(seen) == 1 and not list((registry.manager.directory / "hive-requests").glob("*.json"))


def test_legacy_or_missing_hive_scope_is_not_silently_certified(tmp_path):
    registry, release, _ = fixture(tmp_path)
    registry.manager.register_backend("hive", "snow_real.ads_legacy", "aggregate", "2026-09-01T00:00:00+08:00", now=NOW)
    with pytest.raises(ValueError, match="every exact"):
        cleanup(registry, MemoryRPC(registry, release))


def test_descriptor_tampering_and_failed_cleanup_reservation_are_blocked(tmp_path):
    registry, release, _ = fixture(tmp_path)
    registry.reserve(release, now=NOW)
    data = registry.read()
    value = next(iter(data["tables"].values()))["descriptor"]
    value["collector"]["generation"] = "11111111-1111-4111-8111-111111111111"
    write_json(registry.path, data)
    with pytest.raises(ValueError, match="escaped"):
        registry.read()
    registry.path.unlink()
    write_json(registry.manager.journal, {"fixture": "failed cleanup"})
    with pytest.raises(ValueError, match="Finish failed"):
        registry.reserve(release, now=NOW)


def test_worker_sources_parse_as_python38_and_never_accept_sql():
    root = Path(__file__).parents[1]
    for relative in ("src/snow_statistics/real_hive_contract.py", "src/snow_statistics/real_hive_spark.py", "warehouse/spark/real_hive_catalog.py"):
        ast.parse((root / relative).read_text(encoding="utf-8"), feature_version=(3, 8))
    worker = (root / "warehouse/spark/real_hive_catalog.py").read_text()
    assert 'add_argument("--sql"' not in worker


def test_partition_describe_uses_partition_section_not_table_location():
    from snow_statistics.real_hive_spark import partition_location
    def row(key, value=""):
        return SimpleNamespace(col_name=key, data_type=value)
    root = "hdfs://snow-control:9000/snow/warehouse/real/test/runs/test/ads_daily"
    actual = root + "/source=real/business_date=2026-09-05"
    rows = [row("# Detailed Partition Information"), row("Location", actual), row("# Storage Information"), row("Location", root)]
    assert partition_location(rows, root) == actual
    rows[1] = row("Location", "hdfs://snow-control:9000/elsewhere")
    assert partition_location(rows, root) == "hdfs://snow-control:9000/elsewhere"
    with pytest.raises(ValueError, match="exact"):
        partition_location(rows[2:], root)


def test_describe_columns_does_not_resolve_parquet_files():
    from snow_statistics.real_hive_spark import describe_columns
    def row(key, value=""):
        return SimpleNamespace(col_name=key, data_type=value)
    rows = [row("app", "string"), row("pv", "bigint"), row("source", "string"), row("business_date", "date"),
            row("# Partition Information"), row("# col_name", "data_type"), row("source", "string"), row("business_date", "date"),
            row(""), row("# Detailed Table Information"), row("Provider", "parquet")]
    assert describe_columns(rows) == (["app", "pv", "source", "business_date"], ["source", "business_date"])
    rows[0] = row("app", "struct<unapproved:string>")
    with pytest.raises(ValueError, match="types"):
        describe_columns(rows)


def test_spark_adapter_reads_two_locations_and_issues_only_exact_drop(tmp_path):
    from snow_statistics.real_hive_spark import SparkHiveCatalog, field_type
    registry, release, _ = fixture(tmp_path)
    table, descriptor = next(iter(registry.reserve(release, now=NOW).items()))
    partition = "source=real/business_date=2026-09-05"
    def row(name, kind=""):
        return SimpleNamespace(col_name=name, data_type=kind)
    columns = metadata(descriptor)["columns"]
    describe = [row(name, field_type(name)) for name in columns] + [row("# Partition Information"), row("# col_name", "data_type"),
                row("source", "string"), row("business_date", "date"), row(""), row("# Detailed Table Information"),
                row("Type", "EXTERNAL"), row("Provider", "parquet"), row("Location", descriptor["location"])]
    detail = [row("# Detailed Partition Information"), row("Location", descriptor["location"] + "/" + partition),
              row("# Storage Information"), row("Location", descriptor["location"])]
    queries = []
    class Query:
        def __init__(self, rows):
            self.values = rows
        def limit(self, count):
            self.values = self.values[:count]
            return self
        def collect(self):
            return self.values
    def sql(statement):
        queries.append(statement)
        if statement == "DESCRIBE EXTENDED " + table:
            return Query(describe)
        if statement == "SHOW TBLPROPERTIES " + table:
            return Query([SimpleNamespace(key=key, value=value) for key, value in properties(descriptor).items()])
        if statement == "SHOW PARTITIONS " + table:
            return Query([(partition,)])
        if statement.startswith("DESCRIBE EXTENDED " + table + " PARTITION "):
            return Query(detail)
        if statement == "DROP TABLE " + table:
            return Query([])
        raise AssertionError(statement)
    spark = SimpleNamespace(catalog=SimpleNamespace(tableExists=lambda name: name == table), sql=sql)
    adapter = SparkHiveCatalog(spark)
    actual = adapter.inspect(table)
    assert validate_metadata(descriptor, actual)
    adapter.drop(table)
    assert queries[-1] == "DROP TABLE " + table
    detail[1] = row("Location", "hdfs://snow-control:9000/elsewhere")
    with pytest.raises(ValueError, match="escaped"):
        validate_metadata(descriptor, adapter.inspect(table))


def test_cli_reuses_strict_config_and_never_runs_on_windows(tmp_path, monkeypatch, capsys):
    root = Path(__file__).parents[1]
    spec = importlib.util.spec_from_file_location("real_hive_cli_fixture", root / "tools/real_hive.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    config = json.loads((root / "deploy/real-run.example.json").read_bytes())
    with pytest.raises(ValueError, match="configured transport VM"):
        module.execute(config, "cleanup", root=tmp_path)
    relative = "runtime/real/config/hive-fixture.json"
    write_json(tmp_path / relative, config)
    (tmp_path / relative).chmod(0o600)
    monkeypatch.setattr(module, "ROOT", tmp_path)
    calls = []
    monkeypatch.setattr(module, "execute", lambda value, command, run: calls.append((value, command, run)) or {"fixture_only": True})
    monkeypatch.setattr("sys.argv", ["real_hive.py", "--config", relative, "verify", "--run-id", "test"])
    module.main()
    assert calls[0] == (config, "verify", "test")
    assert json.loads(capsys.readouterr().out) == {"fixture_only": True}
    config["command"] = "arbitrary shell"
    write_json(tmp_path / relative, config)
    (tmp_path / relative).chmod(0o600)
    with pytest.raises(ValueError, match="configuration"):
        module.main()


@pytest.mark.parametrize("origin,node", [
    ("real", "snow-analysis"), ("synthetic fixtures", "snow-analysis"),
    ("synthetic fixtures", "snow-control"),
])
def test_hive_reads_the_authority_release_and_ignores_other_valid_old_pair(tmp_path, monkeypatch, origin, node):
    from snow_statistics.real_publication import read_real_release
    from snow_statistics.real_transfer import paths
    spec = importlib.util.spec_from_file_location("real_hive_release_fixture", Path(__file__).parents[1] / "tools/real_hive.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "read_real_release", lambda path: read_real_release(path, NOW))
    config = dict(input_origin=origin, transport_node=node, lane="fixture-lake-test")
    managed = tmp_path / paths(config["lane"], "test")["published"]
    legacy = tmp_path / "runtime/real/publication"
    selected, other = (managed, legacy) if node == "snow-analysis" else (legacy, managed)
    daily, behavior = packages()
    release_real(daily, behavior, selected, "test", now=NOW)
    release_real(daily, behavior, other, "old", now=NOW)
    assert module.release_directory(config, "test", tmp_path) == selected
    assert read_real_release(other, NOW)["run_id"] == "old"


@pytest.mark.parametrize("managed_exists", [False, True])
def test_analysis_never_falls_back_to_legacy_pair_when_managed_pair_is_missing_or_wrong(tmp_path, monkeypatch, managed_exists):
    from snow_statistics.real_publication import read_real_release
    from snow_statistics.real_transfer import paths
    spec = importlib.util.spec_from_file_location("real_hive_release_failure", Path(__file__).parents[1] / "tools/real_hive.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "read_real_release", lambda path: read_real_release(path, NOW))
    config = dict(input_origin="synthetic fixtures", transport_node="snow-analysis", lane="fixture-lake-test")
    daily, behavior = packages()
    release_real(daily, behavior, tmp_path / "runtime/real/publication", "test", now=NOW)
    if managed_exists:
        release_real(daily, behavior, tmp_path / paths(config["lane"], "test")["published"], "old", now=NOW)
    with pytest.raises((FileNotFoundError, ValueError)):
        module.release_directory(config, "test", tmp_path)


def test_catalog_timeout_signals_owned_process_group_before_return(monkeypatch):
    from snow_statistics import real_hive
    calls = []
    class Process:
        pid = 12345
        def communicate(self, timeout):
            calls.append(("communicate", timeout))
            if len(calls) == 1:
                raise subprocess.TimeoutExpired("fixture", timeout)
            return "", ""
        def poll(self):
            return None
    monkeypatch.setattr(real_hive.subprocess, "Popen", lambda *args, **kwargs: Process())
    # Windows does not expose killpg, while this executable is Linux-only.
    monkeypatch.setattr(real_hive.os, "killpg", lambda pid, signum: calls.append(("signal", pid, signum)), raising=False)
    with pytest.raises(subprocess.TimeoutExpired):
        real_hive.run_catalog_worker(["synthetic-fixture"], cwd=".", timeout=420)
    assert calls[1][0:2] == ("signal", 12345)
    assert calls[2] == ("communicate", 50)
