"""Catalog native-library isolation; synthetic shell checks are not Docker evidence."""
import hashlib
import json
import os
import shlex
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[1]


def catalog_arguments():
    text = (ROOT / 'tools/spark_hive_catalog.sh').read_text(encoding='utf-8')
    command = text[text.index('timeout --signal=TERM --kill-after=10s 240s sudo docker run'):]
    return shlex.split(command.replace('\\\n', ''))


def values(arguments, option):
    return [arguments[i + 1] for i, value in enumerate(arguments) if value == option]


def test_only_snappy_mount_can_execute_and_general_tmp_stays_restricted():
    arguments = catalog_arguments()
    mounts = dict(item.split(':', 1) for item in values(arguments, '--tmpfs'))
    assert set(mounts) == {'/tmp', '/opt/snow-native'}
    ordinary, native = (set(mounts[path].split(',')) for path in ('/tmp', '/opt/snow-native'))
    assert {'rw', 'noexec', 'nosuid', 'nodev', 'size=192m'} == ordinary
    assert {'rw', 'exec', 'nosuid', 'nodev', 'size=16m', 'mode=0700'} == native
    assert '--read-only' in arguments
    assert values(arguments, '--memory') == ['768m']
    assert values(arguments, '--cpus') == ['1'] and values(arguments, '--pids-limit') == ['128']
    assert all(mount.endswith(':ro') for mount in values(arguments, '-v'))


def test_fixed_native_loader_property_does_not_redirect_general_temp_or_bypass_jar_lock():
    arguments = catalog_arguments()
    configs = values(arguments, '--conf')
    options = [value for value in configs if 'extraJavaOptions=' in value]
    assert options == ['spark.driver.extraJavaOptions=-Dorg.xerial.snappy.tempdir=/opt/snow-native']
    assert not any('java.io.tmpdir' in value for value in configs)
    assert arguments[arguments.index('bash') + 1] == '/opt/snow/lab/spark-submit-locked.sh'
    assert values(arguments, '--master') == ['local[1]']
    assert values(arguments, '--driver-memory') == ['512m']
    assert values(arguments, '--label') == ['snow.hive.request=$2']
    assert values(arguments, '--cidfile') == ['$PWD/$cidfile']
    assert values(arguments, '--request') == ['/opt/snow/$1'] and values(arguments, '--sha256') == ['$2']


def bash_executable():
    candidate = Path('C:/Program Files/Git/bin/bash.exe') if os.name == 'nt' else Path(shutil.which('bash') or '')
    if not candidate.is_file():
        pytest.skip('Bash unavailable; no WSL/VM is started for a shell-only test')
    return str(candidate)


@pytest.mark.parametrize('tampered', [False, True])
def test_locked_submit_preserves_whole_java_option_argument_and_rejects_changed_jar(tmp_path, tampered):
    jars = tmp_path / 'jars'
    jars.mkdir()
    body = b'synthetic jar bytes; never loaded by Java'
    (jars / 'synthetic.jar').write_bytes(body + (b' changed' if tampered else b''))
    lock = tmp_path / 'jars.sha256'
    lock.write_text(hashlib.sha256(body).hexdigest() + '  synthetic.jar\n', newline='\n')
    target = tmp_path / 'submit-stub.sh'
    target.write_text('#!/usr/bin/env bash\nprintf \'%s\\0\' "$@"\n', encoding='utf-8', newline='\n')
    # Substitute only fixture paths/terminal command; run all existing lock
    # checks and the actual "$@" forwarding syntax from the checked-in wrapper.
    script = (ROOT / 'lab/spark-submit-locked.sh').read_text(encoding='utf-8')
    script = script.replace('/opt/spark/jars', shlex.quote(jars.as_posix()))
    script = script.replace('/opt/snow/lab/locks/spark-jars.sha256', shlex.quote(lock.as_posix()))
    script = script.replace('/opt/spark/bin/spark-submit', 'bash ' + shlex.quote(target.as_posix()))
    arguments = ['--conf', 'spark.driver.extraJavaOptions=-Dorg.xerial.snappy.tempdir=/opt/snow-native',
                 '--conf', 'synthetic.argument=two words', 'synthetic-job.py']
    result = subprocess.run([bash_executable(), '--noprofile', '--norc', '-c', script, 'synthetic', *arguments],
                            capture_output=True, timeout=10)
    if tampered:
        assert result.returncode != 0 and result.stdout == b''
    else:
        assert result.returncode == 0, json.dumps(result.stderr.decode(errors='replace'))
        assert result.stdout.decode().split('\0') == [*arguments, '']
