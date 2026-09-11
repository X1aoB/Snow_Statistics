#!/usr/bin/env bash
set -euo pipefail
cd /opt/spark/jars
sha256sum --status -c /opt/snow/lab/locks/spark-jars.sha256
test "$(find . -maxdepth 1 -name '*.jar' | wc -l)" -eq "$(wc -l < /opt/snow/lab/locks/spark-jars.sha256)"
exec /opt/spark/bin/spark-submit "$@"
