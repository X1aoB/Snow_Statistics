#!/usr/bin/env bash
set -euo pipefail
if ! test -d /data/name/current; then
  if test -n "$(find /data/name -mindepth 1 -maxdepth 1 -print -quit)"; then
    echo 'Nonempty NameNode volume has no current namespace; refusing to format it' >&2
    exit 1
  fi
  hdfs namenode -format -nonInteractive
fi
exec hdfs namenode
