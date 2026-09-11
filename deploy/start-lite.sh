#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export SNOW_STATE_DIR=/var/lib/snow-statistics/state
if ! mountpoint -q "$SNOW_STATE_DIR"; then
  echo 'Dedicated quota filesystem must be mounted before starting collector.' >&2
  exit 1
fi
test -f .env
exec docker compose --env-file lab/locks/images.env --env-file .env -f deploy/compose.lite.yaml up -d --build
