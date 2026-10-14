"""Co-located, original-permit DIM verification; private results, no model writes.

Run only on the production transport authority. There is no SSH dispatcher,
permit issuer, HDFS output writer, collector or business runtime dependency.
"""
import ipaddress
import json
import os
import re
import select
import signal
import socket
import stat
import subprocess
import sys
import time
import types
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import urlsplit

from .io import atomic_write, digest
from .landing import checked_receipt
from .lifecycle import RealLifecycle, timestamp
from .public_dimensions import validate_bundle
from .publication import canonical, publication_lock
from .real_behavior import validate_coverage
from .real_lab import hdfs_context, metadata_paths, validate_job
from .real_remote_lifecycle import RealRemoteLifecycle, outputs_for

DIM_ARTIFACT = '87f5dbfa297a04e7ac699877354077c1db1ef093aae6a019934634361b97b563'
DIM_SHA = 'fc9f7a4945c2a9e1c281c07fcb72377e3944e0e6e69509cbd1a5ab4b3f8b9616'
SQL_SHA = 'a357162118817c8e5f195eb3f64822771cc5f1670b1d1ccdc1d69b77523f3450'
IMAGE = 'apache/spark@sha256:936ff39fd63e2bb5ed064f0fbe1518198473f1cdbfa2f863d087a9a8e58116ba'
MAX_ROWS = 10000
DRIVER_MIB = 1280
MIN_TOTAL_MIB = 2048
MIN_AVAILABLE_MIB = DRIVER_MIB + 256
LABEL = 'org.snow-statistics.real-dimensions'
GROUP_TERM_SECONDS = 3
GROUP_KILL_SECONDS = 2
PIPE_DRAIN_SECONDS = 1
# Original HDFS readback: status + OPEN + redirect, each <=30s. Exact Docker
# cleanup: inspect10 + stop15 + inspect10 + rm10 + list10. Group/pipe <=6s.
CLEANUP_RESERVE_SECONDS = 160
DATA_FILES = ('descriptor.json', 'descriptor.json.tmp', 'driver.log', 'driver.cid', 'spill', 'validation.json',
              'validation.json.tmp', 'acceptance.json', 'acceptance.json.tmp', 'failure.json', 'failure.json.tmp')
ENGINE_FILES = ('warehouse/spark/real_dimensions.py', 'warehouse/spark/event_input.py',
                'warehouse/spark/ods_input.py', 'warehouse/spark/public_dimensions_at_event.sql',
                'lab/spark-submit-locked.sh', 'lab/locks/images.env', 'lab/locks/spark-jars.sha256',
                'lab/generated/hadoop/core-site.xml', 'lab/generated/hadoop/hdfs-site.xml',
                'src/snow_statistics/real_dimensions.py', 'tools/real_dimensions.py', 'tools/airflow_real_gateway.py')
DESCRIPTOR_FIELDS = ('schema_version source lane run_id attempt nodes input snapshot_id collector cutoff date_from date_to '
                     'original_at expires_at issued_at permit_expires_at dim_artifact dim_sha256 sql_sha256 '
                     'engine_files authority_files data_relative max_input_rows')


def require(value, message):
    if not value:
        raise ValueError(message)


def exact(value, fields):
    require(type(value) is dict and set(value) == set(fields.split()), 'Unexpected DIM metadata fields')


def read(path, limit=4 * 1024**2):
    path = Path(path).absolute()
    info = path.lstat()
    require(path.resolve() == path and stat.S_ISREG(info.st_mode) and info.st_nlink == 1
            and 0 < info.st_size <= limit, 'Bounded ordinary metadata/source file required')
    with path.open('rb') as stream:
        raw = stream.read(limit + 1)
    require(len(raw) <= limit, 'Metadata/source file bound exceeded')
    return raw


def strict_json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, 'Duplicate JSON field')
            result[key] = value
        return result
    def nonfinite(value):
        raise ValueError('Nonfinite JSON value')
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=nonfinite)


def authority_node(config):
    require(os.name == 'posix' and config['input_origin'] == 'real'
            and config['transport_node'] == 'snow-analysis' and socket.gethostname() == 'snow-analysis',
            'Execute on the original snow-analysis production transport authority')


def location(root, config, run_id, attempt):
    metadata_paths(config, run_id)
    require(type(attempt) is str and re.fullmatch(r'[a-z][a-z0-9]{0,31}', attempt), 'Fresh bounded DIM attempt required')
    root = Path(root).absolute()
    result = root / 'runtime/real/dim-verification' / config['lane'] / run_id / attempt
    require(result.resolve() == result, 'Ordinary private DIM scope required')
    return result


def original_authority_files(config, run_id):
    paths = metadata_paths(config, run_id)
    base = 'runtime/real/lifecycle/' + config['lane'] + '/'
    return tuple(paths.values()) + tuple(base + name for name in ('owner.json', 'registry.json', 'last-cleanup.json'))


def gateway_guard(root, job, now):
    path = Path(root) / 'tools/airflow_real_gateway.py'
    raw = read(path, 128 * 1024)
    module = types.ModuleType('real_dim_original_gateway')
    module.__file__ = str(path)
    exec(compile(raw, str(path), 'exec'), module.__dict__)
    # Only validate/build the original command. Never run or replace its writer.
    module.build_command(job, root=Path(root), now=now)


def validate_authority(values, config, run_id, now):
    """Validate original metadata, not a newly invented permission envelope."""
    job = validate_job(values['job'], config, run_id)
    state = values['state']
    snapshot = checked_receipt(state)
    require(type(snapshot['schema_version']) is int and snapshot['schema_version'] == 2
            and snapshot['source'] == 'real' and state['input'] == job['input'], 'Exact original real ODS snapshot required')
    owner, registry, receipt, permit = (values[key] for key in ('owner', 'registry', 'receipt', 'permit'))
    collector = snapshot['identity']['collector']
    require(owner['project'] == 'snow-statistics' and owner['source'] == 'real'
            and {key: owner[key] for key in ('instance_id', 'generation')} == {key: collector[key] for key in ('instance_id', 'generation')}
            and owner['roots'] == {'warehouse': job['warehouse_root'], 'auxiliary': job['auxiliary_root'], 'ods': snapshot['root']},
            'Original physical owner/source generation differs')
    validate_coverage(values['coverage'], job['cutoff'], collector)
    require(type(permit.get('schema_version')) is int and permit['schema_version'] == 1
            and permit.get('source') == 'real' and permit['input_snapshot'] == job['input']
            and permit['outputs'] == outputs_for(job) and permit['auxiliary_sha256'] is None and permit['hive_tables'] == [],
            'Original event-only input/output permit differs')
    issued, expiry = timestamp(permit['issued_at']), timestamp(permit['expires_at'])
    require(issued <= now < expiry <= issued + timedelta(minutes=15), 'Fresh original compute permit required')
    require(receipt['source'] == 'real' and receipt['input_snapshot'] == job['input']
            and receipt['owner'] == {key: owner[key] for key in ('instance_id', 'generation')}
            and receipt['registry_sha256'] == digest(canonical(registry))
            and timestamp(receipt['checked_at']) == issued and receipt == values['last_cleanup'],
            'Original cleanup/registry changed or is stale')
    require(not any(value.get('verification') == 'retired_storage_absent' for value in receipt['backends'].values()),
            'Retired storage cannot grant raw compute reads')
    require(receipt['next_expiry'] is not None and expiry <= timestamp(receipt['next_expiry']), 'Original cleanup deadline differs')
    require(snapshot['batches'], 'No computable original input')
    total = 0
    seen = set()
    for batch in snapshot['batches']:
        count = batch['counts']['events']
        require(type(count) is int and count >= 0 and batch['counts']['quarantine'] == 0, 'Bounded clean original input required')
        require(type(batch['batch_id']) is str and re.fullmatch(r'[a-f0-9]{64}', batch['batch_id'])
                and batch['batch_id'] not in seen, 'Unique original input batch required')
        seen.add(batch['batch_id'])
        origin = timestamp(batch['original_min_accepted_at'])
        require(timestamp(batch['expires_at']) == origin + timedelta(days=7)
                and now < timestamp(batch['expires_at']) and origin <= now, 'Original raw expiry must remain unchanged')
        total += count
    require(0 < total <= MAX_ROWS, 'Original snapshot exceeds the DIM read bound or has no input')
    original = min((timestamp(batch['original_min_accepted_at']) for batch in snapshot['batches']))
    require(expiry <= original + timedelta(days=7), 'Permit outlives original raw input')
    return job, snapshot, original


def authority(root, config, run_id, now):
    paths = metadata_paths(config, run_id)
    manager = RealRemoteLifecycle(Path(root) / 'runtime/real/lifecycle' / config['lane'])
    require(not manager.journal.exists() and not (Path(root) / 'runtime/real/ods' / config['lane'] / 'pending.json').exists()
            and not (Path(root) / 'runtime/real/ods' / config['lane'] / 'cleanup.json').exists(), 'Original cleanup/capture is incomplete')
    owner, registry = manager._read()
    values = {name: strict_json(read(Path(root) / relative)) for name, relative in paths.items()}
    values.update(owner=owner, registry=registry, last_cleanup=strict_json(read(manager.directory / 'last-cleanup.json')))
    permit = values['permit']
    require(digest(read(Path(root) / paths['coverage'])) == permit['coverage_sha256']
            and digest(read(Path(root) / paths['receipt'])) == permit['lifecycle_receipt_sha256'], 'Original permit metadata hashes differ')
    job, snapshot, original = validate_authority(values, config, run_id, now)
    gateway_guard(root, job, now)
    hashes = {name: digest(read(Path(root) / name)) for name in original_authority_files(config, run_id)}
    return job, snapshot, original, permit, hashes


def snapshot_readback(sink, job, snapshot, now):
    path = urlsplit(job['input']).path
    info = sink.request('GET', path, 'GETFILESTATUS').json()['FileStatus']
    require(info['type'] == 'FILE' and type(info['length']) is int and 0 < info['length'] <= 4 * 1024**2,
            'Bounded immutable HDFS snapshot metadata required')
    raw = sink.read(path)
    require(len(raw) == info['length'] and raw == canonical(snapshot), 'Actual HDFS snapshot differs from original job/permit')
    require(all(now < timestamp(batch['expires_at']) for batch in snapshot['batches']), 'Raw input expired during metadata readback')


def engine_files(root):
    values = {name: digest(read(Path(root) / name, 256 * 1024)) for name in ENGINE_FILES}
    require(values['warehouse/spark/public_dimensions_at_event.sql'] == SQL_SHA, 'Pinned DIM SQL differs')
    require([line for line in read(Path(root) / 'lab/locks/images.env').decode().splitlines() if line.startswith('SPARK_IMAGE=')]
            == ['SPARK_IMAGE=' + IMAGE], 'Original locked Spark image required')
    return values


def public_dimension(root):
    path = Path(root) / 'runtime/public-dimensions/real' / DIM_ARTIFACT / 'dimensions.json'
    raw = read(path, 128 * 1024)
    require(digest(raw) == DIM_SHA, 'Pinned real public DIM bundle differs')
    bundle = validate_bundle(strict_json(raw))
    require(bundle['source'] == 'real' and bundle['artifact_id'] == DIM_ARTIFACT and bundle['coverage'] == [],
            'Pinned public DIM preserves unknown history')
    return path


def prepare(root, config, run_id, attempt, *, now=None):
    current = now or datetime.now(UTC)
    authority_node(config)
    directory = location(root, config, run_id, attempt)
    require(not directory.exists(), 'Use a fresh DIM attempt; existing evidence is immutable')
    job, snapshot, origin, permit, hashes = authority(root, config, run_id, current)
    public_dimension(root)
    files = engine_files(root)
    with hdfs_context(config) as (sink, _):
        snapshot_readback(sink, job, snapshot, current)
    local = RealLifecycle(directory / 'data')
    local.initialize()
    for name in DATA_FILES:
        local.register(name, 'raw', origin.isoformat(), now=current)
    local.cleanup(current)
    descriptor = dict(schema_version=1, source='real', lane=config['lane'], run_id=run_id, attempt=attempt, nodes=config['nodes'],
        input=job['input'], snapshot_id=digest(canonical(snapshot)), collector=snapshot['identity']['collector'],
        cutoff=job['cutoff'], date_from=job['date_from'], date_to=job['date_to'], original_at=origin.isoformat(),
        expires_at=(origin + timedelta(days=7)).isoformat(), issued_at=current.isoformat(),
        permit_expires_at=permit['expires_at'], dim_artifact=DIM_ARTIFACT, dim_sha256=DIM_SHA, sql_sha256=SQL_SHA,
        engine_files=files, authority_files=hashes, data_relative=local.root.relative_to(Path(root).absolute()).as_posix(), max_input_rows=MAX_ROWS)
    atomic_write(local.path('descriptor.json'), canonical(descriptor))
    local.cleanup(current)
    return {'source': 'real', 'prepared': True, 'descriptor_sha256': digest(canonical(descriptor)),
            'read_permission_granted_by_prepare': False, 'engine_acceptance_claimed': False}


def check(root, config, run_id, attempt, *, now=None):
    current = now or datetime.now(UTC)
    authority_node(config)
    local = RealLifecycle(location(root, config, run_id, attempt) / 'data')
    descriptor = strict_json(read(local.readable('descriptor.json', current), 65536))
    exact(descriptor, DESCRIPTOR_FIELDS)
    job, snapshot, origin, permit, hashes = authority(root, config, run_id, current)
    require(type(descriptor['schema_version']) is int and descriptor['schema_version'] == 1 and descriptor['source'] == 'real'
            and descriptor['lane'] == config['lane'] and descriptor['run_id'] == run_id and descriptor['attempt'] == attempt
            and descriptor['nodes'] == config['nodes']
            and descriptor['input'] == job['input'] and descriptor['snapshot_id'] == digest(canonical(snapshot))
            and descriptor['collector'] == snapshot['identity']['collector']
            and descriptor['cutoff'] == job['cutoff'] and descriptor['date_from'] == job['date_from'] and descriptor['date_to'] == job['date_to']
            and timestamp(descriptor['original_at']) == origin and timestamp(descriptor['expires_at']) == origin + timedelta(days=7)
            and descriptor['permit_expires_at'] == permit['expires_at'] and descriptor['authority_files'] == hashes
            and timestamp(permit['issued_at']) <= timestamp(descriptor['issued_at']) <= current
            and descriptor['engine_files'] == engine_files(root) and descriptor['dim_artifact'] == DIM_ARTIFACT
            and descriptor['dim_sha256'] == DIM_SHA and descriptor['sql_sha256'] == SQL_SHA
            and type(descriptor['max_input_rows']) is int and descriptor['max_input_rows'] == MAX_ROWS
            and descriptor['data_relative'] == local.root.relative_to(Path(root).absolute()).as_posix(), 'DIM descriptor/source/authority changed')
    public_dimension(root)
    with hdfs_context(config) as (sink, _):
        snapshot_readback(sink, job, snapshot, current)
    registry = local._read()['artifacts']
    require(set(registry) == set(DATA_FILES) and all(entry['kind'] == 'raw' and timestamp(entry['original_at']) == origin
            and timestamp(entry['expires_at']) == origin + timedelta(days=7) for entry in registry.values()), 'Owned log/spill/validation reservation differs')
    return descriptor, local


def resource_preflight(root, meminfo=None, disk_bytes=None):
    text = meminfo if meminfo is not None else Path('/proc/meminfo').read_text()
    def value(name):
        match = re.search(r'^' + name + r':\s+(\d+)\s+kB$', text, re.M)
        require(match is not None, 'Actual guest resource observation required')
        return int(match.group(1))
    total, available = value('MemTotal'), value('MemAvailable')
    require(total >= MIN_TOTAL_MIB * 1024 and available >= MIN_AVAILABLE_MIB * 1024,
            'Analysis guest is too small: require usable 2048 MiB and available driver1280+reserve256 MiB; configure a recorded profile separately')
    if disk_bytes is None:
        info = os.statvfs(root)
        disk_bytes = info.f_bavail * info.f_frsize
    require(disk_bytes >= 384 * 1024**2, 'Guest free disk resource gate failed')
    return dict(guest_total_kib=total, guest_available_kib=available, guest_free_disk_bytes=disk_bytes,
                host_resources_measured=False, continuous_peak_measured=False)


def command(root, descriptor):
    root = Path(root).absolute()
    expected_data = 'runtime/real/dim-verification/' + descriptor['lane'] + '/' + descriptor['run_id'] + '/' + descriptor['attempt'] + '/data'
    require(re.fullmatch(r'[a-z][a-z0-9-]{2,23}', descriptor['lane'])
            and re.fullmatch(r'[A-Za-z0-9_-]{1,100}', descriptor['run_id'])
            and re.fullmatch(r'[a-z][a-z0-9]{0,31}', descriptor['attempt'])
            and descriptor['data_relative'] == expected_data
            and set(descriptor['authority_files']) == set(original_authority_files({'lane': descriptor['lane']}, descriptor['run_id'])),
            'Exact private data and co-located authority mounts required')
    data = root / descriptor['data_relative']
    owner = 'snow-real-dim-' + digest(canonical([descriptor['lane'], descriptor['run_id'], descriptor['attempt']]))[:20]
    args = ['sudo', '-n', 'docker', 'run', '--rm', '--pull=never', '--name', owner, '--cidfile', str(data / 'driver.cid'),
            '--label', LABEL + '=' + owner, '--memory', '1280m', '--cpus', '1', '--pids-limit', '512', '--network', 'host',
            '--user', '0:0', '--read-only', '--tmpfs', '/tmp:rw,noexec,nosuid,nodev,size=64m',
            '--tmpfs', '/opt/snow-native:rw,exec,nosuid,nodev,size=16m,mode=0700',
            '-e', 'HADOOP_CONF_DIR=/etc/hadoop', '-e', 'PYSPARK_PYTHON=/usr/bin/python3',
            '-e', 'SPARK_LOCAL_IP=127.0.0.1', '-e', 'SPARK_LOCAL_HOSTNAME=localhost']
    require(set(descriptor['nodes']) == {'snow-control', 'snow-compute', 'snow-analysis'}, 'Original three node aliases required')
    for name, address in descriptor['nodes'].items():
        ip = ipaddress.IPv4Address(address)
        require(ip.is_private and not ip.is_loopback, 'Original lab node address required')
        args += ['--add-host', name + ':' + str(ip)]
    mounts = [('warehouse/spark', '/opt/snow/warehouse/spark', 'ro'),
              ('lab/spark-submit-locked.sh', '/opt/snow/lab/spark-submit-locked.sh', 'ro'),
              ('lab/locks', '/opt/snow/lab/locks', 'ro'), ('lab/generated/hadoop', '/etc/hadoop', 'ro'),
              ('lab/generated/hadoop', '/opt/snow/lab/generated/hadoop', 'ro'),
              ('runtime/public-dimensions/real/' + DIM_ARTIFACT + '/dimensions.json', '/opt/snow/dimensions.json', 'ro'),
              (descriptor['data_relative'], '/opt/snow/' + descriptor['data_relative'], 'rw')]
    mounts.extend((name, '/opt/snow/' + name, 'ro') for name in
                  ('src/snow_statistics/real_dimensions.py', 'tools/real_dimensions.py', 'tools/airflow_real_gateway.py'))
    mounts.extend((name, '/opt/snow/' + name, 'ro') for name in descriptor['authority_files'])
    for relative, destination, mode in mounts:
        path = root / relative
        require(path.resolve() == path and path.exists(), 'Ordinary exact mount required')
        args += ['-v', str(path) + ':' + destination + ':' + mode]
    args += ['-w', '/tmp', IMAGE, 'bash', '/opt/snow/lab/spark-submit-locked.sh', '--master', 'local[1]', '--driver-memory', '512m',
             '--conf', 'spark.ui.enabled=false', '--conf', 'spark.eventLog.enabled=false', '--conf', 'spark.sql.shuffle.partitions=1',
             '--conf', 'spark.default.parallelism=1', '--conf', 'spark.sql.session.timeZone=UTC', '--conf', 'spark.sql.adaptive.enabled=false',
             '--conf', 'spark.local.dir=/opt/snow/' + descriptor['data_relative'] + '/spill',
             '--conf', 'spark.driver.host=127.0.0.1', '--conf', 'spark.driver.bindAddress=127.0.0.1',
             '--conf', 'spark.driver.extraJavaOptions=-Dorg.xerial.snappy.tempdir=/opt/snow-native',
             '/opt/snow/warehouse/spark/real_dimensions.py', '--descriptor', '/opt/snow/' + descriptor['data_relative'] + '/descriptor.json']
    return args, owner


def cleanup_container(cidfile, owner):
    target = owner
    if cidfile.exists():
        target = read(cidfile, 256).decode().strip()
        require(re.fullmatch(r'[a-f0-9]{64}', target), 'Owned driver CID required')
    found = subprocess.run(['sudo', '-n', 'docker', 'inspect', target], capture_output=True, timeout=10)
    if found.returncode:
        listing = subprocess.run(['sudo', '-n', 'docker', 'ps', '-a', '--no-trunc', '--format', '{{.ID}} {{.Names}}'],
                                 capture_output=True, timeout=10, check=True)
        require(not any(target in line.split() or owner in line.split() for line in listing.stdout.decode().splitlines()), 'Driver cleanup uncertain')
        return
    value = strict_json(found.stdout)[0]
    require(value['Name'] == '/' + owner and value['Config']['Labels'].get(LABEL) == owner
            and re.fullmatch(r'[a-f0-9]{64}', value['Id']) and (target == owner or target == value['Id']), 'Driver owner differs')
    if value['State']['Running']:
        subprocess.run(['sudo', '-n', 'docker', 'stop', '-t', '5', value['Id']], capture_output=True, timeout=15, check=True)
    remaining = subprocess.run(['sudo', '-n', 'docker', 'inspect', value['Id']], capture_output=True, timeout=10)
    if remaining.returncode == 0:
        check_value = strict_json(remaining.stdout)[0]
        require(check_value['Id'] == value['Id'] and check_value['Name'] == '/' + owner
                and check_value['Config']['Labels'].get(LABEL) == owner and check_value['State']['Running'] is False,
                'Stopped driver ownership differs')
        subprocess.run(['sudo', '-n', 'docker', 'rm', value['Id']], capture_output=True, timeout=10, check=True)
    result = subprocess.run(['sudo', '-n', 'docker', 'ps', '-a', '--no-trunc', '--format', '{{.ID}} {{.Names}}'],
                            capture_output=True, timeout=10, check=True)
    require(not any(value['Id'] in line.split() or owner in line.split() for line in result.stdout.decode().splitlines()), 'Owned driver remains')


class Deadline:
    """One original UTC lease also anchored to monotonic time; never reset."""
    def __init__(self, expires, *, now=None, monotonic=None):
        current = now or datetime.now(UTC)
        tick = time.monotonic() if monotonic is None else monotonic
        self.utc = expires
        self.monotonic = tick + (expires - current).total_seconds()

    def remaining(self):
        return min((self.utc - datetime.now(UTC)).total_seconds(), self.monotonic - time.monotonic())

    def driver(self):
        remaining = self.remaining() - CLEANUP_RESERVE_SECONDS
        require(remaining >= 30, 'Original permit lacks time after final admission and bounded cleanup reserve')
        child = object.__new__(Deadline)
        current, tick = datetime.now(UTC), time.monotonic()
        child.utc = min(self.utc - timedelta(seconds=CLEANUP_RESERVE_SECONDS), current + timedelta(seconds=240))
        child.monotonic = min(self.monotonic - CLEANUP_RESERVE_SECONDS, tick + 240)
        return child


def _proc_identity(pid):
    try:
        raw = Path('/proc') / str(pid) / 'stat'
        fields = raw.read_text().rsplit(')', 1)[1].split()
        return dict(pid=pid, state=fields[0], pgid=int(fields[2]), sid=int(fields[3]), start=int(fields[19]))
    except (FileNotFoundError, ProcessLookupError):
        return None


class _PosixScope:
    """The marker leader waits for admission; only its dedicated session is ours."""
    def __init__(self, args):
        launcher = ('import subprocess,sys\n'
                    'if sys.stdin.buffer.read(1)!=b"G": sys.exit(125)\n'
                    'child=subprocess.Popen(sys.argv[1:],stdin=subprocess.DEVNULL)\n'
                    'sys.exit(child.wait())\n')
        self.process = subprocess.Popen([sys.executable, '-c', launcher, *args], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            self.marker = _proc_identity(self.process.pid)
            require(self.marker is not None and self.marker['pgid'] == self.process.pid
                    and self.marker['sid'] == self.process.pid and self.process.pid != os.getpgrp(), 'Dedicated owned process session required')
            self.handle = os.pidfd_open(self.process.pid) if hasattr(os, 'pidfd_open') else None
        except BaseException:
            # The marker is still waiting for G; no driver/descendant was released.
            self.process.kill()
            self.process.wait(timeout=2)
            self.process.stdin.close()
            self.process.stdout.close()
            raise

    def release(self, deadline):
        require(deadline.remaining() > 0, 'Driver deadline closed during process startup')
        self.process.stdin.write(b'G')
        self.process.stdin.close()

    def members(self):
        leader = _proc_identity(self.marker['pid'])
        require(leader is None or leader['start'] == self.marker['start'], 'Owned leader identity changed')
        result = []
        for entry in Path('/proc').iterdir():
            if entry.name.isdigit():
                item = _proc_identity(int(entry.name))
                if item and item['sid'] == self.marker['sid'] and item['state'] not in {'Z', 'X'}:
                    require(item['start'] >= self.marker['start'] and item['pgid'] != os.getpgrp(), 'Foreign process entered owned session')
                    result.append(item)
        return result

    def _signal(self, value, until):
        for pgid in {item['pgid'] for item in self.members()}:
            try:
                os.killpg(pgid, value)
            except ProcessLookupError:
                pass
            except PermissionError:
                # sudo/docker descendants may be root-owned. Revalidate the
                # dedicated session before this exact, finite privileged signal.
                require(any(item['pgid'] == pgid for item in self.members()), 'Owned privileged process group changed')
                budget = until - time.monotonic()
                require(budget > 0, 'Owned process signal budget exhausted')
                result = subprocess.run(['sudo', '-n', '/bin/kill', '--signal', signal.Signals(value).name, '--', '-' + str(pgid)],
                                        stdin=subprocess.DEVNULL, capture_output=True, timeout=min(1, budget))
                require(result.returncode == 0 or not any(item['pgid'] == pgid for item in self.members()),
                        'Owned privileged process group signal failed')

    def stop(self):
        # Always inspect/signals the owned scope, even after its leader exited:
        # a surviving descendant may still retain the stdout pipe.
        try:
            until = time.monotonic() + GROUP_TERM_SECONDS
            self._signal(signal.SIGTERM, until)
            while self.members() and time.monotonic() < until:
                self.process.poll()
                time.sleep(.02)
            until = time.monotonic() + GROUP_KILL_SECONDS
            if self.members():
                self._signal(signal.SIGKILL, until)
            while self.members() and time.monotonic() < until:
                self.process.poll()
                time.sleep(.02)
            require(not self.members(), 'Owned process session cleanup incomplete')
            self.process.wait(timeout=max(.01, until - time.monotonic()))
        finally:
            if self.handle is not None:
                os.close(self.handle)
            if not self.process.stdin.closed:
                self.process.stdin.close()

    def chunk(self, timeout):
        if not select.select([self.process.stdout.fileno()], [], [], timeout)[0]:
            return None
        return os.read(self.process.stdout.fileno(), 4096)


class _WindowsScope:
    """Portable synthetic tests own an unnamed Job handle; authority is POSIX."""
    def __init__(self, args):
        import ctypes
        from ctypes import wintypes
        self.ctypes, self.wintypes = ctypes, wintypes
        self.kernel = ctypes.WinDLL('kernel32', use_last_error=True)
        self.kernel.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
        self.kernel.CreateJobObjectW.restype = wintypes.HANDLE
        self.kernel.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
        self.kernel.SetInformationJobObject.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD]
        self.kernel.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
        self.kernel.QueryInformationJobObject.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD, ctypes.c_void_p]
        self.kernel.CloseHandle.argtypes = [wintypes.HANDLE]
        self.kernel.PeekNamedPipe.argtypes = [wintypes.HANDLE, ctypes.c_void_p, wintypes.DWORD, ctypes.c_void_p, ctypes.POINTER(wintypes.DWORD), ctypes.c_void_p]
        self.job = self.kernel.CreateJobObjectW(None, None)
        require(self.job, 'Owned synthetic process Job handle required')
        # JOBOBJECT_EXTENDED_LIMIT_INFORMATION: LimitFlags follows two int64s.
        info = ctypes.create_string_buffer(144 if ctypes.sizeof(ctypes.c_size_t) == 8 else 112)
        ctypes.cast(ctypes.addressof(info) + 16, ctypes.POINTER(wintypes.DWORD)).contents.value = 0x2000
        if not self.kernel.SetInformationJobObject(self.job, 9, info, len(info)):
            self.kernel.CloseHandle(self.job)
            raise ValueError('Owned Job limit initialization failed')
        try:
            self.process = subprocess.Popen(args, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                creationflags=subprocess.CREATE_NO_WINDOW | 0x00000004)
        except BaseException:
            self.kernel.CloseHandle(self.job)
            raise
        try:
            require(self.kernel.AssignProcessToJobObject(self.job, int(self.process._handle)), 'Owned process Job assignment failed')
        except BaseException:
            self.process.kill()
            self.process.wait(timeout=2)
            self.process.stdout.close()
            self.kernel.CloseHandle(self.job)
            raise

    def release(self, deadline):
        require(deadline.remaining() > 0, 'Driver deadline closed during process startup')
        resume = self.ctypes.WinDLL('ntdll').NtResumeProcess
        resume.argtypes = [self.wintypes.HANDLE]
        resume.restype = self.ctypes.c_long
        require(resume(int(self.process._handle)) == 0, 'Owned suspended process resume failed')

    def stop(self):
        try:
            require(self.kernel.TerminateJobObject(self.job, 125), 'Owned process Job termination failed')
            until = time.monotonic() + GROUP_TERM_SECONDS + GROUP_KILL_SECONDS
            info = self.ctypes.create_string_buffer(48)
            while True:
                require(self.kernel.QueryInformationJobObject(self.job, 1, info, len(info), None), 'Owned Job member query failed')
                active = self.ctypes.cast(self.ctypes.addressof(info) + 40, self.ctypes.POINTER(self.wintypes.DWORD)).contents.value
                if active == 0:
                    break
                require(time.monotonic() < until, 'Owned process Job cleanup incomplete')
                time.sleep(.02)
            self.process.wait(timeout=max(.01, until - time.monotonic()))
        finally:
            self.kernel.CloseHandle(self.job)

    def chunk(self, timeout):
        import msvcrt
        handle = msvcrt.get_osfhandle(self.process.stdout.fileno())
        available = self.wintypes.DWORD()
        if not self.kernel.PeekNamedPipe(handle, None, 0, None, self.ctypes.byref(available), None):
            require(self.ctypes.get_last_error() in {109, 232}, 'Owned private pipe query failed')
            return b''
        if available.value:
            return os.read(self.process.stdout.fileno(), min(available.value, 4096))
        time.sleep(timeout)
        return None


def run_driver(args, log, timeout):
    """One absolute lease, owned process scope and cancellable bounded raw pipe."""
    deadline = timeout if isinstance(timeout, Deadline) else Deadline(datetime.now(UTC) + timedelta(seconds=timeout))
    require(deadline.remaining() > 0, 'Driver deadline closed before process startup')
    scope = _PosixScope(args) if os.name == 'posix' else _WindowsScope(args)
    total, exceeded = 0, False
    eof = False

    def consume(chunk):
        nonlocal total, exceeded
        room = max(0, 1024**2 - total)
        log.write(chunk[:room])
        total += len(chunk)
        exceeded |= total > 1024**2

    try:
        scope.release(deadline)
        while True:
            require(deadline.remaining() > 0 and not exceeded, 'Driver absolute deadline/private log bound exceeded')
            chunk = scope.chunk(min(.05, deadline.remaining()))
            if chunk == b'':
                eof = True
                if scope.process.poll() is not None:
                    break
            elif chunk is not None:
                consume(chunk)
            if scope.process.poll() is not None:
                break
    finally:
        try:
            scope.stop()
            until = time.monotonic() + PIPE_DRAIN_SECONDS
            while not eof and time.monotonic() < until:
                chunk = scope.chunk(min(.05, max(0, until - time.monotonic())))
                if chunk == b'':
                    eof = True
                elif chunk is not None:
                    consume(chunk)
            require(eof, 'Owned descendant/private stdout pipe cleanup incomplete')
        finally:
            scope.process.stdout.close()
    require(not exceeded, 'Private log size exceeded')
    return scope.process.returncode


def execute(root, config, run_id, attempt):
    descriptor, local = check(root, config, run_id, attempt)
    lease = Deadline(min(timestamp(descriptor['permit_expires_at']), timestamp(descriptor['expires_at'])))
    resources = resource_preflight(root)
    for name in ('driver.log', 'driver.cid', 'validation.json', 'validation.json.tmp', 'acceptance.json', 'failure.json', 'spill'):
        require(not local.path(name).exists(), 'Use a fresh prepared attempt after an interrupted execution')
    local.path('spill').mkdir(mode=0o700)
    args, owner = command(root, descriptor)
    manager = Path(root) / 'runtime/real/lifecycle' / config['lane']
    success = False
    exit_code = None
    try:
        # Freeze authoritative metadata updates for this bounded application.
        with publication_lock(manager):
            check(root, config, run_id, attempt)
            deadline = lease.driver()
            # This immutable UTC boundary includes lock/final-check/startup time.
            args += ['--driver-deadline', deadline.utc.isoformat()]
            with local.readable('driver.log').open('xb') as log:
                exit_code = run_driver(args, log, deadline)
            require(exit_code == 0, 'DIM driver failed; inspect the registered private log')
            check(root, config, run_id, attempt)
            result = strict_json(read(local.readable('validation.json'), 65536))
            validate_result(result, descriptor)
            success = True
    finally:
        cleanup_container(local.path('driver.cid'), owner)
        if not success:
            atomic_write(local.path('failure.json'), canonical(dict(source='real', driver_exit_code=exit_code,
                         engine_acceptance_claimed=False, original_evidence_retained=True)))
    acceptance = dict(schema_version=1, source='real', driver_exit_code=exit_code, owned_container_removed=True,
                      descriptor_sha256=digest(canonical(descriptor)), validation_sha256=digest(read(local.readable('validation.json'))),
                      resources=resources, original_at=descriptor['original_at'], expires_at=descriptor['expires_at'],
                      real_dim_verification_accepted=result['projection_rows'] > 0, public_evidence_written=False)
    atomic_write(local.path('acceptance.json'), canonical(acceptance))
    return {'source': 'real', 'completed': True, 'engine_acceptance_claimed': result['projection_rows'] > 0,
            'acceptance_sha256': digest(canonical(acceptance)), 'public_evidence_written': False}


def validate_result(value, descriptor):
    exact(value, 'schema_version source descriptor_sha256 dim_sha256 sql_sha256 cutoff original_at expires_at engine projection_rows modes history_coverage row_payload_returned')
    require(type(value['schema_version']) is int and value['schema_version'] == 1 and value['source'] == 'real'
            and value['descriptor_sha256'] == digest(canonical(descriptor)) and value['dim_sha256'] == DIM_SHA and value['sql_sha256'] == SQL_SHA
            and value['cutoff'] == descriptor['cutoff'] and value['original_at'] == descriptor['original_at']
            and value['expires_at'] == descriptor['expires_at'] and value['history_coverage'] == 'unknown'
            and value['row_payload_returned'] is False, 'Actual private validation binding differs')
    exact(value['engine'], 'spark_version python_version master application_id')
    require(value['engine']['spark_version'] == '3.5.7' and value['engine']['master'] == 'local[1]'
            and re.fullmatch(r'3\.8\.\d{1,3}', value['engine']['python_version'])
            and re.fullmatch(r'local-\d{1,20}', value['engine']['application_id']), 'Actual locked driver identity required')
    count = value['projection_rows']
    require(type(count) is int and 0 <= count <= MAX_ROWS, 'Bounded private comparison required')
    exact(value['modes'], 'at_event current')
    for mode, report in value['modes'].items():
        exact(report, 'comparisons status_counts all_fields_equal executed_plan_sha256')
        exact(report['status_counts'], 'present not_present unknown')
        require(type(report['comparisons']) is int and report['comparisons'] == count and report['all_fields_equal'] is True
                and all(type(v) is int and v >= 0 for v in report['status_counts'].values())
                and sum(report['status_counts'].values()) == count and re.fullmatch(r'[a-f0-9]{64}', report['executed_plan_sha256']),
                'Complete actual private comparison required')
        if mode == 'at_event':
            require(report['status_counts'] == {'present': 0, 'not_present': 0, 'unknown': count}, 'Empty real history cannot become known')
    return value
