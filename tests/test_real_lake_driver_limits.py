"""Fixed lake driver process bounds; Bash fixtures never invoke Docker or a VM."""
import os
import shlex
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[1]
SCRIPT = ROOT / 'tools/real_lake_spark.sh'
IMAGE = 'apache/spark@sha256:936ff39fd63e2bb5ed064f0fbe1518198473f1cdbfa2f863d087a9a8e58116ba'


def values(arguments, option):
    return [arguments[i + 1] for i, arg in enumerate(arguments) if arg == option]


def test_lake_driver_has_finite_pid_headroom_and_original_other_limits():
    text = SCRIPT.read_text(encoding='utf-8')
    command = text[text.index('sudo docker run --name'):]
    args = shlex.split(command.replace('\\\n', ''))
    assert values(args, '--pids-limit') == ['512']
    assert values(args, '--memory') == ['1280m']
    assert values(args, '--cpus') == ['2']
    assert values(args, '--driver-memory') == ['640m']
    assert values(args, '--executor-memory') == ['512m']
    assert values(args, '--num-executors') == ['1']
    assert values(args, '--executor-cores') == ['1']
    assert values(args, '--master') == ['yarn']
    assert values(args, '--deploy-mode') == ['client']
    assert values(args, '--label') == ['org.snow-statistics.lake-driver=$driver_name']
    assert values(args, '--cidfile') == ['$cidfile']
    assert values(args, '--pull') == [] and '--pull=never' in args
    assert args[args.index('bash') + 1] == '/opt/snow/lab/spark-submit-locked.sh'
    assert 'docker system prune' not in text and 'docker container prune' not in text


def bash_executable():
    candidate = Path('C:/Program Files/Git/bin/bash.exe') if os.name == 'nt' else Path(shutil.which('bash') or '')
    if not candidate.is_file():
        pytest.skip('Bash unavailable; no WSL or VM is started')
    return str(candidate)


@pytest.mark.parametrize('exit_code,foreign_label', [(0, False), (9, False), (0, True)])
def test_actual_shell_passes_bounded_args_and_cleans_only_matching_driver(tmp_path, exit_code, foreign_label):
    (tmp_path / 'lab/locks').mkdir(parents=True)
    (tmp_path / 'lab/locks/images.env').write_text('SPARK_IMAGE=' + IMAGE + '\n', newline='\n')
    (tmp_path / 'lab/.env').write_text('CONTROL_IP=192.0.2.1\nCOMPUTE_IP=192.0.2.2\nANALYSIS_IP=192.0.2.3\n', newline='\n')
    relative = 'runtime/real/lake-authority/fixture-lake-01/fixture-test/attempt-test/data/driver.cid'
    cidfile = tmp_path / relative
    cidfile.parent.mkdir(parents=True)
    script = SCRIPT.read_text(encoding='utf-8')
    assert script.count('cd /home/snow/Snow_Statistics') == 1
    script = script.replace('cd /home/snow/Snow_Statistics', 'cd ' + shlex.quote(tmp_path.as_posix()))
    prelude = '''
hostname() { printf '%s\\n' snow-control; }
sudo() {
  test "$1" = docker || return 90
  shift
  case "$1" in
    ps) return 0 ;;
    run)
      shift
      printf '%s\\0' "$@" > run.arguments
      printf '%s\\n' aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa > "$cidfile"
      return TEST_EXIT ;;
    inspect)
      if test "$2" = --format; then printf '%s\\n' TEST_LABEL; fi
      return 0 ;;
    rm)
      test "$2" = -f && test "$3" = aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa || return 91
      printf '%s\\n' "$3" > removed.cid ;;
    *) return 92 ;;
  esac
}
'''
    prelude = prelude.replace('TEST_EXIT', str(exit_code)).replace('TEST_LABEL', 'foreign-owner' if foreign_label else '"$driver_name"')
    result = subprocess.run([bash_executable(), '--noprofile', '--norc', '-c', prelude + script,
        'synthetic', relative, 'snow-real-lake-' + 'b' * 20, 'hdfs://192.0.2.1:9000/synthetic', 'synthetic-job.py'],
        capture_output=True, timeout=10)
    args = (tmp_path / 'run.arguments').read_bytes().decode().split('\0')[:-1]
    assert values(args, '--pids-limit') == ['512'] and values(args, '--memory') == ['1280m']
    assert values(args, '--cpus') == ['2'] and values(args, '--driver-memory') == ['640m']
    if foreign_label:
        assert result.returncode != 0
        assert not (tmp_path / 'removed.cid').exists() and cidfile.exists()
    else:
        assert result.returncode == exit_code, result.stderr.decode(errors='replace')
        assert (tmp_path / 'removed.cid').read_text().strip() == 'a' * 64
        assert not cidfile.exists()
