#!/usr/bin/env bash
set -euo pipefail
cd /opt/spark/jars
sha256sum --status -c /snow/spark-jars.sha256
test "$(find . -maxdepth 1 -name '*.jar' | wc -l)" -eq "$(wc -l < /snow/spark-jars.sha256)"
exec yarn nodemanager
