#!/usr/bin/env bash
set -euo pipefail
test "$(hostname)" = snow-control
cd /home/snow/Snow_Statistics
bash tools/setup_airflow_gateway.sh
sudo docker compose --env-file lab/locks/images.env --env-file lab/.env -f lab/compose.airflow.yaml up -d
