"""Python 3.8 driver: original ODS reader, transient DIM views, private comparison.

No collector, model writer, Hive registration or HDFS output operation exists.
The coordinator mounts the original authority metadata itself, never copies.
"""
import argparse
import hashlib
import json
import os
import re
import stat
import sys
import threading
import time
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path('/opt/snow')
DIM_SHA = 'fc9f7a4945c2a9e1c281c07fcb72377e3944e0e6e69509cbd1a5ab4b3f8b9616'
DIM_ARTIFACT = '87f5dbfa297a04e7ac699877354077c1db1ef093aae6a019934634361b97b563'
SQL_SHA = 'a357162118817c8e5f195eb3f64822771cc5f1670b1d1ccdc1d69b77523f3450'
MAX_ROWS = 10000
CLEANUP_RESERVE_SECONDS = 160
ENGINE_FILES = {'warehouse/spark/real_dimensions.py', 'warehouse/spark/event_input.py',
                'warehouse/spark/ods_input.py', 'warehouse/spark/public_dimensions_at_event.sql',
                'lab/spark-submit-locked.sh', 'lab/locks/images.env', 'lab/locks/spark-jars.sha256',
                'lab/generated/hadoop/core-site.xml', 'lab/generated/hadoop/hdfs-site.xml',
                'src/snow_statistics/real_dimensions.py', 'tools/real_dimensions.py', 'tools/airflow_real_gateway.py'}
DATA_FILES = {'descriptor.json', 'descriptor.json.tmp', 'driver.log', 'driver.cid', 'spill', 'validation.json',
              'validation.json.tmp', 'acceptance.json', 'acceptance.json.tmp', 'failure.json', 'failure.json.tmp'}
SELECTED = 'source app event_type occurred_at accepted_at path character_id'.split()
PROJECTION = 'fact_ordinal source app kind dimension_key occurred_at'.split()
RESULT = 'fact_ordinal source app kind release_id dimension_id dimension_status'.split()
SCHEMAS = {
    'public_dim_coverage': ('coverage', 'source string,app string,release_id string,covered_from string,covered_to string'),
    'public_dim_observation': ('observations', 'source string,app string,release_id string,observed_at string,checked_through string'),
    'public_dim_scd2': ('dimensions', 'source string,app string,kind string,dimension_key string,release_id string,dimension_id string,present boolean'),
    'public_dim_fact_projection': (None, 'fact_ordinal long,source string,app string,kind string,dimension_key string,occurred_at string'),
}
CURRENT_SQL = '''
WITH covered AS (
 SELECT DISTINCT f.fact_ordinal, f.source, f.app, f.kind, f.dimension_key, o.release_id
 FROM public_dim_fact_projection f LEFT JOIN public_dim_observation o
 ON f.source=o.source AND f.app=o.app
 AND o.observed_at<=f.occurred_at AND f.occurred_at<=o.checked_through
)
SELECT f.fact_ordinal, f.source, f.app, f.kind, f.release_id, d.dimension_id,
 CASE WHEN f.release_id IS NULL THEN 'unknown'
 WHEN d.present=TRUE THEN 'present' ELSE 'not_present' END AS dimension_status
FROM covered f LEFT JOIN public_dim_scd2 d
 ON f.source=d.source AND f.app=d.app AND f.release_id=d.release_id
 AND f.kind=d.kind AND f.dimension_key=d.dimension_key
'''


def require(value, message):
    if not value:
        raise ValueError(message)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True).encode()


def instant(value):
    require(type(value) is str and len(value) <= 40, 'Bounded explicit timestamp required')
    at = datetime.fromisoformat(value.replace('Z', '+00:00'))
    require(at.tzinfo is not None, 'Timezone required')
    return at.astimezone(timezone.utc)


def utc(value):
    return instant(value).isoformat(timespec='microseconds')


def read(path, limit=4 * 1024**2):
    path = Path(path).absolute()
    info = path.lstat()
    require(path.resolve() == path and stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and 0 < info.st_size <= limit,
            'Bounded ordinary mounted metadata required')
    with path.open('rb') as stream:
        raw = stream.read(limit + 1)
    require(len(raw) <= limit, 'Mounted metadata size exceeded')
    return raw


def decode(raw):
    def pairs(items):
        value = {}
        for key, item in items:
            require(key not in value, 'Duplicate metadata key')
            value[key] = item
        return value
    def nonfinite(value):
        raise ValueError('Nonfinite metadata')
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=nonfinite)


class Guard:
    def __init__(self, descriptor_path, driver_deadline=None):
        path = Path(descriptor_path)
        raw = read(path, 65536)
        descriptor = decode(raw)
        relative = descriptor['data_relative']
        require(re.fullmatch(r'runtime/real/dim-verification/[a-z][a-z0-9-]{2,23}/[A-Za-z0-9_-]{1,100}/[a-z][a-z0-9]{0,31}/data', relative)
                and path == ROOT / relative / 'descriptor.json' and raw == canonical(descriptor)
                and descriptor['source'] == 'real' and type(descriptor['schema_version']) is int and descriptor['schema_version'] == 1,
                'Original authority descriptor path required')
        require(relative == 'runtime/real/dim-verification/' + descriptor['lane'] + '/' + descriptor['run_id'] + '/' + descriptor['attempt'] + '/data'
                and set(descriptor['engine_files']) == ENGINE_FILES, 'Exact local attempt and installed engine source required')
        require(descriptor['dim_sha256'] == DIM_SHA and descriptor['dim_artifact'] == DIM_ARTIFACT
                and descriptor['sql_sha256'] == SQL_SHA and type(descriptor['max_input_rows']) is int and descriptor['max_input_rows'] == MAX_ROWS,
                'Pinned DIM/SQL and bounded original input required')
        self.path, self.raw, self.value, self.data = path, raw, descriptor, path.parent
        latest = min(instant(descriptor['permit_expires_at']), instant(descriptor['expires_at'])) - timedelta(seconds=CLEANUP_RESERVE_SECONDS)
        self.deadline = instant(driver_deadline) if driver_deadline is not None else latest
        require(instant(descriptor['issued_at']) < self.deadline <= latest, 'Driver deadline cannot consume cleanup/original lease budget')
        self.monotonic_deadline = time.monotonic() + (self.deadline - datetime.now(timezone.utc)).total_seconds()
        self.expired = threading.Event()
        self.check()

    def remaining(self):
        return min((self.deadline - datetime.now(timezone.utc)).total_seconds(), self.monotonic_deadline - time.monotonic())

    def time_check(self):
        require(not self.expired.is_set() and self.remaining() > 0, 'Absolute original driver deadline closed')

    def check(self):
        self.time_check()
        d = self.value
        now = datetime.now(timezone.utc)
        require(read(self.path, 65536) == self.raw and instant(d['issued_at']) <= now < instant(d['permit_expires_at'])
                and now < instant(d['expires_at']) and instant(d['expires_at']) == instant(d['original_at']) + timedelta(days=7),
                'Original permit/raw input deadline closed')
        lane, run = d['lane'], d['run_id']
        expected = {'runtime/real/jobs/' + run + '.json', 'runtime/real/coverage/' + run + '.json',
                    'runtime/real/permits/' + run + '.json', 'runtime/real/permits/' + run + '.lifecycle.json',
                    'runtime/real/ods/' + lane + '/state.json'}
        expected |= {'runtime/real/lifecycle/' + lane + '/' + name for name in ('owner.json', 'registry.json', 'last-cleanup.json')}
        require(set(d['authority_files']) == expected, 'Exact co-located original authority files required')
        for name, checksum in d['authority_files'].items():
            require(sha(read(ROOT / name)) == checksum, 'Original authority metadata changed')
        for name, checksum in d['engine_files'].items():
            require(sha(read(ROOT / name, 256 * 1024)) == checksum, 'Bound installed source/engine file changed')
        registry = decode(read(self.data / 'registry.json', 65536))
        owner = decode(read(self.data / '.snow-real-owner.json', 65536))
        gate = decode(read(self.data / 'gate.json', 65536))
        require(owner['source'] == 'real' and owner['project'] == 'snow-statistics' and gate['open'] is True
                and set(registry['artifacts']) == DATA_FILES, 'Owned private log/spill/output scope required')
        for item in registry['artifacts'].values():
            require(item['kind'] == 'raw' and instant(item['original_at']) == instant(d['original_at'])
                    and instant(item['expires_at']) == instant(d['expires_at']), 'Private scope renewed original expiry')
        permit = decode(read(ROOT / ('runtime/real/permits/' + run + '.json'), 65536))
        require(permit['source'] == 'real' and permit['input_snapshot'] == d['input']
                and permit['expires_at'] == d['permit_expires_at']
                and instant(permit['issued_at']) <= now < instant(permit['expires_at']) <= instant(permit['issued_at']) + timedelta(minutes=15),
                'Live original exact input permit required')
        self.time_check()


class ActionWatch:
    """Cancel an in-flight Spark action at the same immutable driver deadline."""
    def __init__(self, spark, guard):
        self.spark, self.guard = spark, guard
        self.done = threading.Event()
        self.errors = []
        def watch():
            while not self.done.wait(.02):
                if guard.remaining() <= 0:
                    guard.expired.set()
                    try:
                        spark.sparkContext.cancelAllJobs()
                    except BaseException:
                        self.errors.append('action_cancellation')
                    return
        self.thread = threading.Thread(target=watch, daemon=True)
        self.thread.start()

    def close(self):
        self.done.set()
        self.thread.join(timeout=1)
        require(not self.thread.is_alive() and not self.errors, 'Spark action cancellation cleanup incomplete')


class GuardedFrame:
    """Original read_events' separate count actions each need fresh admission."""
    ACTIONS = {'count', 'collect', 'first', 'head', 'take', 'show', 'toPandas', 'foreach', 'foreachPartition'}

    def __init__(self, frame, guard, frame_type):
        self.frame, self.guard, self.frame_type = frame, guard, frame_type

    def __getattr__(self, name):
        member = getattr(self.frame, name)
        if not callable(member):
            return member
        def invoke(*args, **kwargs):
            if name in self.ACTIONS or name == 'toLocalIterator':
                self.guard.check()
            result = member(*args, **kwargs)
            if name in self.ACTIONS:
                self.guard.check()
            if name == 'toLocalIterator':
                def iterate():
                    try:
                        while True:
                            self.guard.check()
                            try:
                                row = next(result)
                            except StopIteration:
                                return
                            self.guard.check()
                            yield row
                    finally:
                        close = getattr(result, 'close', None)
                        if close is not None:
                            close()
                return iterate()
            if isinstance(result, self.frame_type):
                return GuardedFrame(result, self.guard, self.frame_type)
            return result
        return invoke


class GuardedReader:
    def __init__(self, reader, guard, frame_type):
        self.reader, self.guard, self.frame_type = reader, guard, frame_type

    def __getattr__(self, name):
        member = getattr(self.reader, name)
        def invoke(*args, **kwargs):
            self.guard.check()
            result = member(*args, **kwargs)
            self.guard.check()
            if isinstance(result, self.frame_type):
                return GuardedFrame(result, self.guard, self.frame_type)
            if result is self.reader:
                return self
            return result
        return invoke


class OriginalSpark:
    def __init__(self, spark, guard, frame_type):
        self.spark, self.guard, self.frame_type = spark, guard, frame_type

    @property
    def read(self):
        self.guard.check()
        return GuardedReader(self.spark.read, self.guard, self.frame_type)

    def __getattr__(self, name):
        return getattr(self.spark, name)


def checked_event_reader(guard):
    modules = {}
    for name in ('ods_input', 'event_input'):
        relative = 'warehouse/spark/' + name + '.py'
        raw = read(ROOT / relative, 128 * 1024)
        require(sha(raw) == guard.value['engine_files'][relative], 'Original input reader source differs')
        module = types.ModuleType('dim_original_' + name)
        module.__file__ = str(ROOT / relative)
        exec(compile(raw, module.__file__, 'exec'), module.__dict__)
        modules[name] = module
        if name == 'ods_input':
            sys.modules['ods_input'] = module
    return modules['event_input'].read_events


def project(rows, bundle, descriptor):
    allowed = {(r['app'], r['kind'], r['dimension_key']) for r in bundle['dimensions']}
    result, seen = [], 0
    source = descriptor['source']
    hk = timezone(timedelta(hours=8))
    for row in rows:
        seen += 1
        require(seen <= MAX_ROWS and type(row) is dict and set(row) == set(SELECTED), 'Only bounded identifier-free selected input is allowed')
        require(row['source'] == source and row['app'] in {'mywebsite', 'project_snow'}, 'Selected source/application differs')
        require(instant(descriptor['original_at']) <= instant(row['accepted_at']) <= instant(descriptor['cutoff']), 'Original accepted_at cutoff/provenance differs')
        day = instant(row['occurred_at']).astimezone(hk).date().isoformat()
        if not descriptor['date_from'] <= day <= descriptor['date_to']:
            continue
        kind, key = None, None
        if row['event_type'] == 'page_view':
            require(row['path'] is not None and row['character_id'] is None, 'Page projection shape differs')
            kind, key = 'page', row['path']
        elif row['event_type'] in {'character_select', 'request_complete'}:
            require(row['app'] == 'project_snow' and row['path'] is None and row['character_id'] is not None, 'Character projection shape differs')
            kind, key = 'character', row['character_id']
        else:
            require(row['event_type'] in {'entry_click', 'entry_arrival', 'request_observed'}
                    and row['path'] is None and row['character_id'] is None, 'Original event shape differs')
        if kind is not None:
            require(type(key) is str and (row['app'], kind, key) in allowed, 'Key outside public application/kind whitelist')
            result.append(dict(fact_ordinal=len(result), source=source, app=row['app'], kind=kind,
                               dimension_key=key, occurred_at=utc(row['occurred_at'])))
    return result


def expected_rows(projection, bundle, mode, as_of):
    output = []
    for f in projection:
        at = f['occurred_at'] if mode == 'at_event' else utc(as_of)
        table = bundle['coverage' if mode == 'at_event' else 'observations']
        releases = set()
        for row in table:
            if row['source'] == f['source'] and row['app'] == f['app']:
                inside = row['covered_from'] <= at < row['covered_to'] if mode == 'at_event' else row['observed_at'] <= at <= row['checked_through']
                if inside:
                    releases.add(row['release_id'])
        require(len(releases) <= 1, 'Conflicting explicit public DIM coverage')
        rid = next(iter(releases)) if releases else None
        rows = [r for r in bundle['dimensions'] if r['source'] == f['source'] and r['app'] == f['app'] and r['kind'] == f['kind']
                and r['dimension_key'] == f['dimension_key'] and r['release_id'] == rid]
        require(len(rows) <= 1, 'Ambiguous public dimension')
        row = rows[0] if rows else None
        status = 'unknown' if rid is None else 'present' if row and row['present'] is True else 'not_present'
        output.append(dict(fact_ordinal=f['fact_ordinal'], source=f['source'], app=f['app'], kind=f['kind'], release_id=rid,
                           dimension_id=row['dimension_id'] if row else None, dimension_status=status))
    return output


def compare(actual, expected):
    require(len(actual) == len(expected) <= MAX_ROWS and all(type(r) is dict and set(r) == set(RESULT)
            and type(r['fact_ordinal']) is int for r in actual), 'Complete bounded identifier-free results required')
    require(len({r['fact_ordinal'] for r in actual}) == len(actual), 'Join duplicated temporary ordinals')
    require(sorted(actual, key=lambda r: r['fact_ordinal']) == sorted(expected, key=lambda r: r['fact_ordinal']), 'Complete private DIM field comparison differs')
    counts = {key: sum(r['dimension_status'] == key for r in actual) for key in ('present', 'not_present', 'unknown')}
    return dict(comparisons=len(actual), status_counts=counts, all_fields_equal=True)


def view(spark, name, rows):
    schema = SCHEMAS[name][1]
    fields = [item.split()[0] for item in schema.split(',')]
    spark.createDataFrame([tuple(row[field] for field in fields) for row in rows], schema).createOrReplaceTempView(name)


def worker(descriptor_path, driver_deadline):
    guard = Guard(descriptor_path, driver_deadline)
    d = guard.value
    require(sys.version_info[:2] == (3, 8), 'Locked engine Python 3.8 required')
    raw = read(ROOT / 'dimensions.json', 128 * 1024)
    require(sha(raw) == DIM_SHA, 'Pinned public DIM bytes differ')
    bundle = decode(raw)
    require(bundle['source'] == 'real' and bundle['artifact_id'] == DIM_ARTIFACT and bundle['coverage'] == [], 'Real historical coverage remains unknown')
    sql = read(ROOT / 'warehouse/spark/public_dimensions_at_event.sql', 65536).decode('utf-8')
    require(sha(sql.encode('utf-8')) == SQL_SHA, 'Pinned historical SQL differs')
    from pyspark.sql import DataFrame, SparkSession
    spark = SparkSession.builder.appName('snow-real-dimensions-readonly').getOrCreate()
    watch = ActionWatch(spark, guard)
    frame = None
    try:
        require(spark.version == '3.5.7' and spark.sparkContext.master == 'local[1]', 'Actual locked local Spark identity required')
        guard.check()
        frame, quality, _, snapshot = checked_event_reader(guard)(OriginalSpark(spark, guard, DataFrame), d['input'], 'real', d['cutoff'])
        require(quality['quarantined'] == 0 and quality['raw'] <= MAX_ROWS and snapshot['source'] == 'real'
                and snapshot['snapshot_id'] == d['snapshot_id'] and snapshot['collector'] == d['collector'], 'Original reader snapshot/source/quality differs')
        guard.check()
        # Drop every original event/request/user/session/jump identifier BEFORE
        # the bounded iterator. Original reader caching uses the owned spill root.
        selected = frame.select(*SELECTED).limit(MAX_ROWS + 1)
        projection = project((row.asDict(recursive=True) for row in selected.toLocalIterator()), bundle, d)
        guard.check()
        for name, (table, _) in SCHEMAS.items():
            if table is not None:
                view(spark, name, bundle[table])
        reports = {}
        for mode, statement in [('at_event', sql), ('current', CURRENT_SQL)]:
            guard.check()
            facts = [dict(f) for f in projection]
            if mode == 'current':
                for fact in facts:
                    fact['occurred_at'] = utc(d['cutoff'])
            view(spark, 'public_dim_fact_projection', facts)
            result = spark.sql(statement)
            actual = [row.asDict(recursive=True) for row in result.limit(MAX_ROWS + 1).collect()]
            guard.check()
            report = compare(actual, expected_rows(projection, bundle, mode, d['cutoff']))
            report['executed_plan_sha256'] = sha(result._jdf.queryExecution().executedPlan().toString().encode('utf-8'))
            reports[mode] = report
        output = dict(schema_version=1, source='real', descriptor_sha256=sha(guard.raw), dim_sha256=DIM_SHA, sql_sha256=SQL_SHA,
                      cutoff=d['cutoff'], original_at=d['original_at'], expires_at=d['expires_at'],
                      engine=dict(spark_version=spark.version, python_version=sys.version.split()[0], master=spark.sparkContext.master,
                                  application_id=spark.sparkContext.applicationId), projection_rows=len(projection), modes=reports,
                      history_coverage='unknown', row_payload_returned=False)
        guard.check()
        target = guard.data / 'validation.json'
        temporary = guard.data / 'validation.json.tmp'
        require(not target.exists() and not temporary.exists(), 'Immutable private validation already exists')
        with temporary.open('xb') as stream:
            stream.write(canonical(output))
            stream.flush()
            os.fsync(stream.fileno())
        guard.check()
        os.replace(temporary, target)
    finally:
        try:
            if frame is not None:
                frame.unpersist(blocking=True)
            for name in SCHEMAS:
                spark.catalog.dropTempView(name)
            spark.stop()
        finally:
            watch.close()
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--descriptor')
    parser.add_argument('--driver-deadline')
    args = parser.parse_args(argv)
    if args.descriptor is None:
        print(json.dumps(dict(mode='describe_only', input_read=False, engine_started=False)))
        return 0
    require(args.driver_deadline is not None, 'Coordinator absolute driver deadline required')
    return worker(args.descriptor, args.driver_deadline)


if __name__ == '__main__':
    raise SystemExit(main())
