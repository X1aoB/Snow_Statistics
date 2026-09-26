"""Isolated candidate module and synthetic inspect replies, no Docker or VM."""
import copy
import itertools
import json

import pytest

from snow_statistics import real_lake_stage as stage


def container(service):
    return {
        'Id': ('a' if service == 'datanode' else 'b') * 64,
        'Name': '/snow-lab-compute-' + service + '-1',
        'Image': 'sha256:' + 'c' * 64,
        'Config': {'Image': 'locked-hadoop' if service == 'datanode' else 'snow-yarn-spark:0.1.0',
                   'Labels': {'com.docker.compose.project': 'snow-lab-compute',
                              'com.docker.compose.service': service},
                   'Env': ['SYNTHETIC_FIXED=value']},
        'HostConfig': {'Memory': (384 if service == 'datanode' else 1536) * 1024**2,
                       'NetworkMode': 'host', 'Binds': ['/synthetic/conf:/conf:ro']},
        'Mounts': [
            {'Type': 'volume', 'Name': 'd' * 64, 'Source': '/var/lib/docker/volumes/' + 'd' * 64 + '/_data',
             'Destination': '/data', 'Driver': 'local', 'Mode': '', 'RW': True, 'Propagation': ''},
            {'Type': 'volume', 'Name': 'synthetic-owned-data',
             'Source': '/var/lib/docker/volumes/synthetic-owned-data/_data',
             'Destination': '/data/dn', 'Driver': 'local', 'Mode': 'z', 'RW': True, 'Propagation': ''},
            {'Type': 'bind', 'Source': '/synthetic/conf', 'Destination': '/conf',
             'Mode': 'ro', 'RW': False, 'Propagation': 'rprivate'},
        ],
        'State': {'OOMKilled': False, 'Running': True, 'StartedAt': '2026-09-19T00:00:00Z'},
        'RestartCount': 0,
    }


@pytest.fixture
def docker(tmp_path):
    path = tmp_path / 'lab/locks/images.env'
    path.parent.mkdir(parents=True)
    path.write_text('HADOOP_IMAGE=locked-hadoop\nHIVE_IMAGE=locked-hive\n')
    value = object.__new__(stage.StageDocker)
    value.root = tmp_path
    value.rows = {name: container(name) for name in ('datanode', 'nodemanager')}
    value.actions = []
    value.memory = lambda: (1714 * 1024, 512 * 1024)

    def run(*args, timeout=20):
        if args[0] == 'inspect':
            if len(args) == 2 and args[1] in {row['Id'] for row in value.rows.values()}:
                rows = [row for row in value.rows.values() if row['Id'] == args[1]]
            else:
                assert set(args[1:]) == {row['Name'].removeprefix('/') for row in value.rows.values()}
                rows = list(value.rows.values())
            return json.dumps(rows).encode()
        if args[0] == 'ps':
            assert args == ('ps', '--format', '{{.Names}}')
            return '\n'.join(row['Name'].removeprefix('/') for row in value.rows.values()
                             if row['State']['Running']).encode()
        assert args[0:3] == ('stop', '-t', '20') and timeout == 35
        value.actions.append(args)
        next(row for row in value.rows.values() if row['Id'] == args[3])['State']['Running'] = False
        return b''

    value.run = run
    return value


def test_all_mount_permutations_keep_complete_identity_without_mutating_input():
    value = container('datanode')
    saved = copy.deepcopy(value)
    expected = stage.container_hash(value)
    for mounts in itertools.permutations(value['Mounts']):
        reordered = copy.deepcopy(value)
        reordered['Mounts'] = list(mounts)
        assert stage.container_hash(reordered) == expected
    assert value == saved


def test_actual_snapshot_boundary_and_owned_stop_accept_only_order_change(docker):
    baseline, _ = stage.checked_snapshot('snow-compute', docker)
    for row in docker.rows.values():
        row['Mounts'].reverse()
    current, _ = stage.checked_snapshot('snow-compute', docker, baseline)
    assert current == baseline
    docker.stop_owned(baseline['nodemanager'])
    assert docker.actions == [('stop', '-t', '20', docker.rows['nodemanager']['Id'])]
    assert docker.rows['datanode']['State']['Running']
    assert not docker.rows['nodemanager']['State']['Running']


@pytest.mark.parametrize('key,replacement', [
    ('Source', '/foreign/source'), ('Destination', '/foreign-target'), ('Type', 'bind'),
    ('Name', 'foreign-volume'), ('Driver', 'foreign-driver'), ('Mode', 'ro'),
    ('RW', False), ('Propagation', 'shared'), ('ExtraField', {'nested': [1, 2]}),
])
def test_every_mount_field_change_is_still_rejected_by_snapshot_and_stop(docker, key, replacement):
    baseline, _ = stage.checked_snapshot('snow-compute', docker)
    docker.rows['nodemanager']['Mounts'][0][key] = replacement
    with pytest.raises(ValueError, match='identity changed'):
        stage.checked_snapshot('snow-compute', docker, baseline)
    with pytest.raises(ValueError, match='changed stage container'):
        docker.stop_owned(baseline['nodemanager'])
    assert docker.actions == []


@pytest.mark.parametrize('change', ['removed', 'duplicate', 'extra'])
def test_mount_membership_and_multiplicity_are_not_lost(docker, change):
    baseline = docker.snapshot('snow-compute')
    mounts = docker.rows['nodemanager']['Mounts']
    if change == 'removed':
        mounts.pop()
    elif change == 'duplicate':
        mounts.append(copy.deepcopy(mounts[0]))
    else:
        mounts.append({'Type': 'tmpfs', 'Destination': '/unexpected', 'RW': True})
    with pytest.raises(ValueError, match='changed stage container'):
        docker.stop_owned(baseline['nodemanager'])
    assert docker.actions == []


@pytest.mark.parametrize('field', ['Id', 'Name', 'Image', 'Config', 'HostConfig'])
def test_other_identity_fields_remain_part_of_the_digest(field):
    value = container('datanode')
    changed = copy.deepcopy(value)
    changed[field] = {'changed': True} if isinstance(changed[field], dict) else 'changed'
    assert stage.container_hash(value) != stage.container_hash(changed)
