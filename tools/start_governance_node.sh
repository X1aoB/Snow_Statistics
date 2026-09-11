#!/usr/bin/env bash
set -euo pipefail
test "$(hostname)" = snow-analysis
cd /home/snow/Snow_Statistics
for service in $(sudo docker ps --filter label=com.docker.compose.project=snow-lab-analysis --format '{{.Label "com.docker.compose.service"}}'); do
  case "$service" in marquez|governance-db) ;; *) echo "Stop $service before governance" >&2; exit 1 ;; esac
done
sudo docker compose --env-file lab/locks/images.env --env-file lab/.env -f lab/compose.analysis.yaml --profile governance up -d governance-db marquez
