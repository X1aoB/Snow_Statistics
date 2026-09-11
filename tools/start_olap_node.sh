#!/usr/bin/env bash
set -euo pipefail
test "$(hostname)" = snow-analysis
cd /home/snow/Snow_Statistics
if sudo docker ps --filter label=com.docker.compose.project=snow-lab-analysis --format '{{.Label "com.docker.compose.service"}}' | grep -qx datanode; then
  echo 'Stop the batch DataNode before switching to the olap profile' >&2
  exit 1
fi
sudo docker compose --env-file lab/locks/images.env --env-file lab/.env \
  -f lab/compose.analysis.yaml -f lab/compose.olap-small.yaml --profile olap up -d doris-fe doris-be
