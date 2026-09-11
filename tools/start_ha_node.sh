#!/usr/bin/env bash
set -euo pipefail
test "$(hostname)" = snow-analysis
cd /home/snow/Snow_Statistics
profile=${1:?Supply kafka-ha or zookeeper}
case "$profile" in kafka-ha|zookeeper) ;; *) exit 2 ;; esac
test -z "$(sudo docker ps -q)"
test "$(awk '/MemTotal/ {print $2}' /proc/meminfo)" -ge 2800000
test -f lab/secrets/ha.env
set -a
. lab/locks/images.env
set +a
image="$KAFKA_IMAGE"
if test "$profile" = zookeeper; then image="$ZOOKEEPER_IMAGE"; fi
if ! sudo docker image inspect "$image" >/dev/null 2>&1; then
  test "$(df -Pm / | awk 'NR==2 {print $4}')" -ge 1024
  sudo docker pull "$image" </dev/null
fi
sudo docker compose --env-file lab/locks/images.env --env-file lab/secrets/ha.env \
  -f lab/compose.ha.json --profile "$profile" up -d --no-build --pull never </dev/null
