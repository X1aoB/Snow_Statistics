"""Synthetic guards and SQL fixtures; no real facts, HDFS, Docker or VM."""
import ast
import copy
import io
import json
import os
import signal
import sqlite3
import sys
import types
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit

import pytest

from snow_statistics import real_dimensions as dim
from snow_statistics.io import digest, write_json
from snow_statistics.landing import checked_receipt
from snow_statistics.public_dimensions import DimensionIndex, build_bundle
from snow_statistics.publication import canonical
from snow_statistics.real_lab import metadata_paths
from snow_statistics.real_remote_lifecycle import RealRemoteLifecycle, outputs_for

ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 1, 4, tzinfo=UTC)
IDENTITY = dict(schema_version=1, source='real', instance_id='fbf85904-0a98-44f4-bd13-804280831449',
                generation='c2b34a4a-a18d-47ac-bfe5-acb41444e3d4')


def load(path):
    module = types.ModuleType('dim_test_' + path.stem)
    module.__file__ = str(path)
    exec(compile(path.read_bytes(), str(path), 'exec'), module.__dict__)
    return module


driver = load(ROOT / 'warehouse/spark/real_dimensions.py')
cli = load(ROOT / 'tools/real_dimensions.py')


def fixture(tmp_path):
    """Fabricated real-labelled metadata exercises original admission contracts."""
    config = json.loads((ROOT / 'deploy/real-run.example.json').read_bytes())
    config['lane'] = 'fixture-dim'
    paths = metadata_paths(config, 'dim_fixture')
    root = 'hdfs://' + config['nodes']['snow-control'] + ':9000/snow/'
    snapshot = dict(schema_version=2, source='real', identity={'collector': IDENTITY}, offsets={'fixture:0': 1},
                    batches=[dict(batch_id='a' * 64, counts={'events': 1, 'quarantine': 0},
                                  original_min_accepted_at=(NOW - timedelta(days=1)).isoformat(),
                                  expires_at=(NOW + timedelta(days=6)).isoformat())],
                    root=root + 'ods/real/kafka/fixture-dim', head_batch_id='a' * 64)
    token = digest(canonical(snapshot))
    state = snapshot | dict(snapshot_id=token, batch_id='a' * 64, input=snapshot['root'] + '/snapshots/' + token + '/_snapshot.json')
    job = dict(run_id='dim_fixture', kind='daily', source='real', input=state['input'], warehouse_root=root + 'warehouse/real/fixture-dim',
               auxiliary_root=root + 'auxiliary/real/fixture-dim', date_from='2026-01-01', date_to='2026-01-03', cutoff=NOW.isoformat(),
               coverage_file='dim_fixture.json', auxiliary_file=None, permit_file='dim_fixture.json', register_hive=False)
    coverage = dict(schema_version=1, source='real', instance_id=IDENTITY['instance_id'], generation=IDENTITY['generation'],
                    continuous_from=(NOW - timedelta(days=3)).isoformat(), through=NOW.isoformat(), gaps=[])
    manager = RealRemoteLifecycle(tmp_path / 'runtime/real/lifecycle/fixture-dim')
    manager.initialize(job['warehouse_root'], job['auxiliary_root'], snapshot['root'], IDENTITY['instance_id'], IDENTITY['generation'])
    owner, registry = manager._read()
    receipt = dict(schema_version=1, source='real', owner={key: IDENTITY[key] for key in ('instance_id', 'generation')},
                   checked_at=NOW.isoformat(), registry_sha256=digest(canonical(registry)), input_snapshot=job['input'],
                   backends={name: {'scope': 'not_initialized', 'certified': False} for name in registry['backends']},
                   next_expiry=(NOW + timedelta(days=6)).isoformat())
    for name, value in [('job', job), ('coverage', coverage), ('state', state), ('receipt', receipt)]:
        write_json(tmp_path / paths[name], value)
    permit = dict(schema_version=1, source='real', input_snapshot=job['input'], outputs=outputs_for(job),
                  coverage_sha256=digest((tmp_path / paths['coverage']).read_bytes()), auxiliary_sha256=None, hive_tables=[],
                  issued_at=NOW.isoformat(), expires_at=(NOW + timedelta(minutes=10)).isoformat(),
                  lifecycle_receipt_sha256=digest((tmp_path / paths['receipt']).read_bytes()))
    write_json(tmp_path / paths['permit'], permit)
    write_json(manager.directory / 'last-cleanup.json', receipt)
    values = dict(job=job, coverage=coverage, state=state, receipt=receipt, permit=permit,
                  owner=owner, registry=registry, last_cleanup=copy.deepcopy(receipt))
    for name in dim.ENGINE_FILES:
        destination = tmp_path / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        if name.startswith('lab/generated/hadoop/'):
            destination.write_text('<configuration/>', encoding='utf-8')
        else:
            destination.write_bytes((ROOT / name).read_bytes())
    return config, values, manager


def test_exact_original_authority_validates_without_inventing_a_permit(tmp_path):
    config, values, _ = fixture(tmp_path)
    job, snapshot, original = dim.validate_authority(values, config, 'dim_fixture', NOW)
    assert job == values['job'] and snapshot == checked_receipt(values['state'])
    assert original == NOW - timedelta(days=1)
    assert dim.authority(tmp_path, config, 'dim_fixture', NOW)[0] == job


@pytest.mark.parametrize('mutation', [
    lambda v: v['permit'].update(source='synthetic'),
    lambda v: v['permit'].update(schema_version=True),
    lambda v: v['permit'].update(input_snapshot='hdfs://foreign/input'),
    lambda v: v['permit'].update(expires_at=NOW.isoformat()),
    lambda v: v['permit'].update(expires_at=(NOW + timedelta(minutes=16)).isoformat()),
    lambda v: v['permit']['outputs'].update(daily='hdfs://foreign/output'),
    lambda v: v['owner'].update(generation='00000000-0000-0000-0000-000000000001'),
    lambda v: v['receipt'].update(registry_sha256='f' * 64),
    lambda v: v['receipt'].update(checked_at=(NOW - timedelta(hours=1)).isoformat()),
    lambda v: v['last_cleanup'].update(input_snapshot='foreign'),
    lambda v: v['receipt']['backends']['kafka'].update(verification='retired_storage_absent'),
    lambda v: v['job'].update(dimensions_file='unapproved.json'),
])
def test_wrong_source_expired_stale_and_other_outputs_refused(tmp_path, mutation):
    config, values, _ = fixture(tmp_path)
    mutation(values)
    with pytest.raises(ValueError):
        dim.validate_authority(values, config, 'dim_fixture', NOW)


def test_pending_cleanup_and_permit_hash_mismatch_refused(tmp_path):
    config, _, manager = fixture(tmp_path)
    (manager.directory / 'cleanup.json').write_text('{}')
    with pytest.raises(ValueError, match='incomplete'):
        dim.authority(tmp_path, config, 'dim_fixture', NOW)
    (manager.directory / 'cleanup.json').unlink()
    coverage = tmp_path / metadata_paths(config, 'dim_fixture')['coverage']
    coverage.write_bytes(coverage.read_bytes() + b' ')
    with pytest.raises(ValueError, match='hashes'):
        dim.authority(tmp_path, config, 'dim_fixture', NOW)


def test_actual_authority_node_requires_original_analysis_not_a_copied_receipt(monkeypatch):
    config = json.loads((ROOT / 'deploy/real-run.example.json').read_bytes())
    monkeypatch.setattr(dim, 'os', types.SimpleNamespace(name='posix'))
    monkeypatch.setattr(dim.socket, 'gethostname', lambda: 'snow-control')
    with pytest.raises(ValueError):
        dim.authority_node(config)
    monkeypatch.setattr(dim.socket, 'gethostname', lambda: 'snow-analysis')
    dim.authority_node(config)
    config['input_origin'] = 'synthetic fixtures'
    with pytest.raises(ValueError):
        dim.authority_node(config)


def test_low_analysis_profile_cannot_fit_driver_and_reserve():
    with pytest.raises(ValueError, match='too small'):
        dim.resource_preflight(ROOT, 'MemTotal: 1048576 kB\nMemAvailable: 921600 kB\n', 1024**3)
    with pytest.raises(ValueError):
        dim.resource_preflight(ROOT, 'MemTotal: 2359296 kB\nMemAvailable: 1500000 kB\n', 1024**3)
    resource = dim.resource_preflight(ROOT, 'MemTotal: 2359296 kB\nMemAvailable: 1800000 kB\n', 1024**3)
    assert resource['host_resources_measured'] is False and resource['continuous_peak_measured'] is False


def test_prepare_reserves_every_private_output_and_spill_before_any_driver(tmp_path, monkeypatch):
    config, values, _ = fixture(tmp_path)
    monkeypatch.setattr(dim, 'authority_node', lambda cfg: None)
    monkeypatch.setattr(dim, 'public_dimension', lambda root: None)
    observed = []

    class Sink:
        def request(self, method, path, op):
            observed.append((method, path, op))
            body = canonical(checked_receipt(values['state']))
            return types.SimpleNamespace(json=lambda: {'FileStatus': {'type': 'FILE', 'length': len(body)}})

        def read(self, path):
            assert path == urlsplit(values['job']['input']).path
            return canonical(checked_receipt(values['state']))

    @contextmanager
    def context(cfg):
        yield Sink(), None

    monkeypatch.setattr(dim, 'hdfs_context', context)
    monkeypatch.setattr(dim.subprocess, 'Popen', lambda *args, **kwargs: pytest.fail('Prepare started driver'))
    result = dim.prepare(tmp_path, config, 'dim_fixture', 'r1', now=NOW)
    assert result['read_permission_granted_by_prepare'] is False and len(observed) == 1
    descriptor, local = dim.check(tmp_path, config, 'dim_fixture', 'r1', now=NOW)
    assert set(local._read()['artifacts']) == set(dim.DATA_FILES)
    assert all(entry['kind'] == 'raw' and entry['original_at'] == descriptor['original_at']
               and entry['expires_at'] == descriptor['expires_at'] for entry in local._read()['artifacts'].values())
    assert not local.path('spill').exists() and not local.path('driver.log').exists()
    assert not (tmp_path / 'runtime/real/publication').exists()
    class SyntheticClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW if tz is not None else NOW.replace(tzinfo=None)
    monkeypatch.setattr(driver, 'ROOT', tmp_path)
    monkeypatch.setattr(driver, 'datetime', SyntheticClock)
    guard = driver.Guard(str(local.path('descriptor.json')))
    assert guard.value['input'] == values['job']['input']
    permit_path = tmp_path / metadata_paths(config, 'dim_fixture')['permit']
    before = permit_path.read_bytes()
    permit_path.write_bytes(before + b' ')
    with pytest.raises(ValueError, match='changed'):
        guard.check()
    permit_path.write_bytes(before)
    # A copied/new approval field never replaces the original registered gate.
    write_json(local.root / 'gate.json', {'open': False, 'read_approved': True})
    with pytest.raises(ValueError, match='Owned'):
        guard.check()
    local.cleanup(NOW)
    with pytest.raises(ValueError, match='fresh'):
        dim.prepare(tmp_path, config, 'dim_fixture', 'r1', now=NOW)


def test_command_has_only_owned_writable_scope_locked_readonly_and_native_tmpfs(tmp_path):
    config, _, _ = fixture(tmp_path)
    data = dim.location(tmp_path, config, 'dim_fixture', 'r1') / 'data'
    data.mkdir(parents=True)
    bundle = tmp_path / 'runtime/public-dimensions/real' / dim.DIM_ARTIFACT / 'dimensions.json'
    bundle.parent.mkdir(parents=True)
    bundle.write_text('{}')
    descriptor = dict(lane=config['lane'], run_id='dim_fixture', attempt='r1', nodes=config['nodes'], data_relative=data.relative_to(tmp_path).as_posix(),
                      authority_files={name: 'a' * 64 for name in dim.original_authority_files(config, 'dim_fixture')})
    args, owner = dim.command(tmp_path, descriptor)
    assert dim.IMAGE in args and '--read-only' in args and '--pull=never' in args
    assert args[args.index('--memory') + 1] == '1280m' and args[args.index('--pids-limit') + 1] == '512'
    assert '/opt/snow-native:rw,exec,nosuid,nodev,size=16m,mode=0700' in args
    assert 'spark.eventLog.enabled=false' in args and '--master' in args and 'local[1]' in args
    assert str(tmp_path) + ':/opt/snow' not in ' '.join(args)
    mounts = [args[index + 1] for index, value in enumerate(args) if value == '-v']
    assert len([mount for mount in mounts if mount.endswith(':rw')]) == 1
    assert next(mount for mount in mounts if mount.endswith(':rw')).startswith(str(data) + ':')
    assert owner.startswith('snow-real-dim-')
    descriptor['authority_files']['runtime/real/secrets/reader.token'] = 'f' * 64
    with pytest.raises(ValueError):
        dim.command(tmp_path, descriptor)


def test_original_snapshot_readback_refuses_changed_bytes_before_payload(tmp_path):
    _, values, _ = fixture(tmp_path)
    body = canonical(checked_receipt(values['state']))
    sink = types.SimpleNamespace(request=lambda *args: types.SimpleNamespace(json=lambda: {'FileStatus': {'type': 'FILE', 'length': len(body)}}),
                                 read=lambda path: body)
    dim.snapshot_readback(sink, values['job'], checked_receipt(values['state']), NOW)
    sink.read = lambda path: body + b' '
    with pytest.raises(ValueError):
        dim.snapshot_readback(sink, values['job'], checked_receipt(values['state']), NOW)


def synthetic_dim():
    spec = json.loads((ROOT / 'examples/public-dimensions.synthetic.json').read_bytes())
    spec['coverage'] = []
    return build_bundle(spec)


def selected():
    return dict(source='synthetic', app='mywebsite', event_type='page_view', occurred_at='2026-01-01T02:00:00Z',
                accepted_at='2026-01-03T23:00:00Z', path='/old', character_id=None)


def descriptor_fixture():
    return dict(source='synthetic', original_at='2026-01-01T00:00:00Z', cutoff='2026-01-04T00:00:00Z',
                date_from='2026-01-01', date_to='2026-01-03')


@pytest.mark.parametrize('identifier', ['anonymous_id', 'event_id', 'request_id', 'session_id', 'jump_id', 'seq'])
def test_all_original_identifiers_refused_before_dim_projection(identifier):
    row = selected() | {identifier: 'private-canary'}
    with pytest.raises(ValueError):
        driver.project([row], synthetic_dim(), descriptor_fixture())


def test_transient_ordinals_preserve_distinct_facts_and_empty_history_unknown():
    bundle = synthetic_dim()
    rows = driver.project([selected(), selected()], bundle, descriptor_fixture())
    assert [row['fact_ordinal'] for row in rows] == [0, 1]
    assert all(set(row) == set(driver.PROJECTION) for row in rows)
    expected = driver.expected_rows(rows, bundle, 'at_event', descriptor_fixture()['cutoff'])
    assert all(row['release_id'] is None and row['dimension_id'] is None and row['dimension_status'] == 'unknown' for row in expected)
    assert driver.compare(expected, expected) == {'comparisons': 2, 'status_counts': {'present': 0, 'not_present': 0, 'unknown': 2}, 'all_fields_equal': True}
    index = DimensionIndex(bundle)
    for fact in rows:
        assert index.resolve(source='synthetic', app=fact['app'], kind=fact['kind'], key=fact['dimension_key'], at=fact['occurred_at'])['status'] == 'unknown'


def test_source_cutoff_public_key_boundary_and_complete_result_negatives():
    bundle = synthetic_dim()
    for update in ({'source': 'real'}, {'path': '/not-public'}, {'accepted_at': '2026-01-04T00:00:00.000001Z'}, {'occurred_at': '2026-01-01T02:00:00'}):
        with pytest.raises(ValueError):
            driver.project([selected() | update], bundle, descriptor_fixture())
    p = driver.project([selected(), selected()], bundle, descriptor_fixture())
    expected = driver.expected_rows(p, bundle, 'at_event', descriptor_fixture()['cutoff'])
    bad = copy.deepcopy(expected)
    bad[1]['fact_ordinal'] = 0
    with pytest.raises(ValueError):
        driver.compare(bad, expected)
    bad = copy.deepcopy(expected)
    bad[0]['dimension_id'] = 'changed'
    with pytest.raises(ValueError):
        driver.compare(bad, expected)


def test_python38_driver_and_default_cli_do_not_open_inputs_or_engine(monkeypatch, capsys):
    ast.parse((ROOT / 'warehouse/spark/real_dimensions.py').read_bytes(), feature_version=(3, 8))
    def forbidden(*args, **kwargs):
        pytest.fail('Describe opened input or started process')
    monkeypatch.setattr(Path, 'open', forbidden)
    monkeypatch.setattr(dim.subprocess, 'Popen', forbidden)
    assert cli.main(['--config', 'missing', '--run-id', 'missing']) == 0 and driver.main([]) == 0
    for line in capsys.readouterr().out.splitlines():
        assert json.loads(line)['input_read'] is False


def test_private_log_size_and_process_deadline_are_bounded():
    log = io.BytesIO()
    assert dim.run_driver([sys.executable, '-c', 'print("synthetic-only-log")'], log, 2) == 0
    assert b'synthetic-only-log' in log.getvalue()
    log = io.BytesIO()
    with pytest.raises(ValueError):
        dim.run_driver([sys.executable, '-c', 'print("x"*1048577)'], log, 2)
    assert len(log.getvalue()) <= 1024**2
    with pytest.raises(ValueError):
        dim.run_driver([sys.executable, '-c', 'import time;time.sleep(10)'], io.BytesIO(), .05)


def test_sql_views_match_private_reference_without_exposing_rows():
    bundle = synthetic_dim()
    projection = driver.project([selected()], bundle, descriptor_fixture())
    for mode in ('at_event', 'current'):
        facts = copy.deepcopy(projection)
        if mode == 'current':
            for fact in facts:
                fact['occurred_at'] = driver.utc(descriptor_fixture()['cutoff'])
        with sqlite3.connect(':memory:') as db:
            for view, (table, schema) in driver.SCHEMAS.items():
                fields = [item.split()[0] for item in schema.split(',')]
                db.execute('CREATE TABLE ' + view + '(' + ','.join(fields) + ')')
                rows = facts if table is None else bundle[table]
                db.executemany('INSERT INTO ' + view + ' VALUES (' + ','.join('?' for _ in fields) + ')', [[r[f] for f in fields] for r in rows])
            sql = (ROOT / 'warehouse/spark/public_dimensions_at_event.sql').read_text() if mode == 'at_event' else driver.CURRENT_SQL
            cursor = db.execute(sql)
            names = [item[0] for item in cursor.description]
            actual = [dict(zip(names, row)) for row in cursor.fetchall()]
        report = driver.compare(actual, driver.expected_rows(projection, bundle, mode, descriptor_fixture()['cutoff']))
        assert 'fact_ordinal' not in report and 'dimension_key' not in report and report['all_fields_equal'] is True


def result_fixture():
    descriptor = dict(cutoff=NOW.isoformat(), original_at=(NOW - timedelta(days=1)).isoformat(), expires_at=(NOW + timedelta(days=6)).isoformat())
    report = dict(comparisons=2, status_counts={'present': 0, 'not_present': 0, 'unknown': 2}, all_fields_equal=True,
                  executed_plan_sha256='a' * 64)
    value = dict(schema_version=1, source='real', descriptor_sha256=digest(canonical(descriptor)), dim_sha256=dim.DIM_SHA,
                 sql_sha256=dim.SQL_SHA, cutoff=descriptor['cutoff'], original_at=descriptor['original_at'], expires_at=descriptor['expires_at'],
                 engine=dict(spark_version='3.5.7', python_version='3.8.10', master='local[1]', application_id='local-1234567890123'),
                 projection_rows=2, modes={'at_event': copy.deepcopy(report), 'current': copy.deepcopy(report)},
                 history_coverage='unknown', row_payload_returned=False)
    return descriptor, value


@pytest.mark.parametrize('mutation', [
    lambda v: v.update(source='synthetic'),
    lambda v: v.update(row_payload_returned=True),
    lambda v: v.update(rows=[{'anonymous_id': 'private-canary'}]),
    lambda v: v.update(projection_rows=True),
    lambda v: v.update(expires_at=(NOW + timedelta(days=90)).isoformat()),
    lambda v: v['engine'].update(master='yarn'),
    lambda v: v['engine'].update(python_version='3.12.0'),
    lambda v: v['modes']['at_event']['status_counts'].update(present=1, unknown=1),
    lambda v: v['modes']['current'].update(comparisons=1),
    lambda v: v['modes']['current'].update(all_fields_equal=False),
])
def test_private_result_cannot_extend_expiry_claim_history_or_export_rows(mutation):
    descriptor, value = result_fixture()
    dim.validate_result(value, descriptor)
    mutation(value)
    with pytest.raises(ValueError):
        dim.validate_result(value, descriptor)


def test_duplicate_nonfinite_metadata_and_wrong_mount_scope_refused():
    for raw in (b'{"source":"real","source":"synthetic"}', b'{"value":NaN}'):
        with pytest.raises(ValueError):
            dim.strict_json(raw)
        with pytest.raises(ValueError):
            driver.decode(raw)


def test_every_guard_engine_path_has_a_real_readonly_mount(tmp_path):
    config, _, _ = fixture(tmp_path)
    data = dim.location(tmp_path, config, 'dim_fixture', 'r1') / 'data'
    data.mkdir(parents=True)
    public = tmp_path / 'runtime/public-dimensions/real' / dim.DIM_ARTIFACT / 'dimensions.json'
    public.parent.mkdir(parents=True)
    public.write_text('{}')
    descriptor = dict(lane=config['lane'], run_id='dim_fixture', attempt='r1', nodes=config['nodes'],
        data_relative=data.relative_to(tmp_path).as_posix(),
        authority_files={name: 'a' * 64 for name in dim.original_authority_files(config, 'dim_fixture')})
    args, _ = dim.command(tmp_path, descriptor)
    mounts = [args[i + 1].rsplit(':', 2)[1:] for i, value in enumerate(args) if value == '-v']
    assert ['/etc/hadoop', 'ro'] in mounts and ['/opt/snow/lab/generated/hadoop', 'ro'] in mounts
    for relative in dim.ENGINE_FILES:
        target = PurePosixPath('/opt/snow') / relative
        assert any(mode == 'ro' and (target == PurePosixPath(destination) or PurePosixPath(destination) in target.parents)
                   for destination, mode in mounts), relative


@pytest.mark.parametrize('lease_seconds,delay,log_delay,expected', [(100, 70, 0, None), (400, 70, 20, 150)])
def test_final_admission_and_log_startup_consume_one_absolute_lease(tmp_path, monkeypatch, lease_seconds, delay, log_delay, expected):
    clock = {'now': NOW, 'tick': 1000.0}
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock['now']
    def advance(seconds):
        clock['now'] += timedelta(seconds=seconds)
        clock['tick'] += seconds
    monkeypatch.setattr(dim, 'datetime', Clock)
    monkeypatch.setattr(dim.time, 'monotonic', lambda: clock['tick'])
    data = tmp_path / 'data'
    data.mkdir()
    def readable(name, *args):
        if name == 'driver.log':
            advance(log_delay)
        return data / name
    local = types.SimpleNamespace(path=lambda name: data / name, readable=readable)
    descriptor = dict(permit_expires_at=(NOW + timedelta(seconds=lease_seconds)).isoformat(),
                      expires_at=(NOW + timedelta(days=1)).isoformat())
    calls = []
    def check(*args, **kwargs):
        calls.append(True)
        if len(calls) == 2:
            advance(delay)
        return descriptor, local
    monkeypatch.setattr(dim, 'check', check)
    monkeypatch.setattr(dim, 'resource_preflight', lambda root: {})
    monkeypatch.setattr(dim, 'command', lambda *args: (['synthetic-driver'], 'owned-fixture'))
    @contextmanager
    def lock(*args):
        yield
    monkeypatch.setattr(dim, 'publication_lock', lock)
    monkeypatch.setattr(dim, 'cleanup_container', lambda *args: None)
    observed = []
    class ReachedDriver(RuntimeError):
        pass
    def run(args, log, deadline):
        observed.append(deadline.remaining())
        assert '--driver-deadline' in args
        assert deadline.utc <= NOW + timedelta(seconds=lease_seconds - dim.CLEANUP_RESERVE_SECONDS)
        raise ReachedDriver('synthetic test ends before any subprocess')
    monkeypatch.setattr(dim, 'run_driver', run)
    if expected is None:
        with pytest.raises(ValueError, match='after final admission'):
            dim.execute(tmp_path, {'lane': 'fixture-dim'}, 'dim_fixture', 'r1')
        assert observed == []
    else:
        with pytest.raises(ReachedDriver):
            dim.execute(tmp_path, {'lane': 'fixture-dim'}, 'dim_fixture', 'r1')
        assert observed == [expected]


def test_deadline_cannot_be_extended_by_wall_clock_rollback(monkeypatch):
    clock = {'now': NOW, 'tick': 1000.0}
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock['now']
    monkeypatch.setattr(dim, 'datetime', Clock)
    monkeypatch.setattr(dim.time, 'monotonic', lambda: clock['tick'])
    lease = dim.Deadline(NOW + timedelta(seconds=400))
    clock['now'] -= timedelta(hours=1)
    clock['tick'] += 200
    budget = lease.driver()
    assert budget.remaining() == 40
    clock['tick'] += 40
    assert budget.remaining() <= 0
    assert dim.CLEANUP_RESERVE_SECONDS >= 90 + 55 + dim.GROUP_TERM_SECONDS + dim.GROUP_KILL_SECONDS + dim.PIPE_DRAIN_SECONDS
    assert dim.CLEANUP_RESERVE_SECONDS == driver.CLEANUP_RESERVE_SECONDS


def test_posix_scope_signals_owned_descendants_even_after_marker_parent_exited(monkeypatch):
    scope = object.__new__(dim._PosixScope)
    scope.marker = dict(pid=901, pgid=901, sid=901, start=100, state='S')
    scope.handle = None
    scope.process = types.SimpleNamespace(poll=lambda: 0, wait=lambda timeout: 0, stdin=io.BytesIO())
    owned = dict(pid=902, pgid=902, sid=901, start=101, state='S')
    foreign = dict(pid=903, pgid=111, sid=111, start=102, state='S')
    identities = {901: None, 902: owned, 903: foreign}
    monkeypatch.setattr(dim, '_proc_identity', lambda pid: identities.get(pid))
    monkeypatch.setattr(dim, 'Path', lambda root: types.SimpleNamespace(iterdir=lambda: [types.SimpleNamespace(name=str(pid)) for pid in identities]))
    signals = []
    def send(pgid, value):
        signals.append((pgid, value))
        assert pgid == 902
        owned['state'] = 'X'
    monkeypatch.setattr(dim, 'os', types.SimpleNamespace(getpgrp=lambda: 111, killpg=send))
    scope.stop()
    assert signals == [(902, signal.SIGTERM)] and scope.process.stdin.closed


def test_posix_scope_rejects_reused_marker_before_signalling(monkeypatch):
    scope = object.__new__(dim._PosixScope)
    scope.marker = dict(pid=901, pgid=901, sid=901, start=100, state='S')
    monkeypatch.setattr(dim, '_proc_identity', lambda pid: dict(pid=901, pgid=901, sid=901, start=999, state='S'))
    with pytest.raises(ValueError, match='identity changed'):
        scope.members()


def process_alive(pid):
    if os.name == 'posix':
        info = dim._proc_identity(pid)
        return info is not None and info['state'] not in {'Z', 'X'}
    import ctypes
    from ctypes import wintypes
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel.OpenProcess.restype = wintypes.HANDLE
    kernel.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    handle = kernel.OpenProcess(0x00100000, False, pid)
    if not handle:
        return False
    try:
        return kernel.WaitForSingleObject(handle, 0) == 258
    finally:
        kernel.CloseHandle(handle)


@pytest.mark.parametrize('parent_exits', [False, True])
def test_descendant_holding_stdout_is_stopped_and_pipe_drained(tmp_path, parent_exits):
    pidfile = tmp_path / 'owned-synthetic-child.pid'
    program = ('import pathlib,subprocess,sys,time;'
               'child=subprocess.Popen([sys.executable,"-c","import time;time.sleep(20)"]);'
               'pathlib.Path(sys.argv[1]).write_text(str(child.pid));'
               + ('sys.exit(0)' if parent_exits else 'time.sleep(20)'))
    if parent_exits:
        assert dim.run_driver([sys.executable, '-c', program, str(pidfile)], io.BytesIO(), 2) == 0
    else:
        with pytest.raises(ValueError, match='deadline'):
            dim.run_driver([sys.executable, '-c', program, str(pidfile)], io.BytesIO(), .7)
    assert pidfile.exists()
    assert not process_alive(int(pidfile.read_text()))


def test_each_separate_original_reader_action_is_admitted_and_late_action_denied():
    state = {'actions': 0, 'checks': 0, 'expired': False}
    class Guard:
        def check(self):
            state['checks'] += 1
            if state['expired']:
                raise ValueError('synthetic lease expired')
    class Frame:
        def cache(self):
            return self
        def filter(self, *args):
            return self
        def count(self):
            state['actions'] += 1
            if state['actions'] == 2:
                state['expired'] = True
            return 1
    class Reader:
        def schema(self, *args):
            return self
        def json(self, *args):
            return Frame()
    spark = driver.OriginalSpark(types.SimpleNamespace(read=Reader()), Guard(), Frame)
    frame = spark.read.schema('synthetic schema').json('synthetic metadata').cache()
    assert frame.filter('synthetic predicate').count() == 1
    with pytest.raises(ValueError, match='lease expired'):
        frame.count()
    with pytest.raises(ValueError, match='lease expired'):
        frame.count()
    assert state['actions'] == 2 and state['checks'] >= 8


def test_inflight_spark_action_is_cancelled_at_its_absolute_deadline():
    import threading
    import time
    ended = threading.Event()
    end = time.monotonic() + .06
    class Guard:
        expired = threading.Event()
        def remaining(self):
            return end - time.monotonic()
        def check(self):
            if self.expired.is_set() or self.remaining() <= 0:
                raise ValueError('synthetic action deadline')
    class Frame:
        def count(self):
            assert ended.wait(1), 'The in-flight action was not cancelled'
            return 1
    guard = Guard()
    spark = types.SimpleNamespace(sparkContext=types.SimpleNamespace(cancelAllJobs=ended.set))
    watch = driver.ActionWatch(spark, guard)
    try:
        with pytest.raises(ValueError, match='action deadline'):
            driver.GuardedFrame(Frame(), guard, Frame).count()
    finally:
        watch.close()
    assert ended.is_set() and not watch.thread.is_alive()
