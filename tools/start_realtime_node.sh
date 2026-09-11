#!/usr/bin/env bash
set -euo pipefail
test "$(hostname)" = snow-analysis
cd /home/snow/Snow_Statistics
test "$(awk '/MemTotal/ {print $2}' /proc/meminfo)" -ge 4000000
for name in $(sudo docker ps --format '{{.Names}}'); do
  case "$name" in snow-lab-realtime-*|snow-lab-analysis-doris-fe-1|snow-lab-analysis-doris-be-1) ;;
    *) echo "Stop $name before realtime acceptance" >&2; exit 1 ;;
  esac
done
test -f lab/secrets/realtime.env
# Reuse matching containers: recreating the large BE copy-on-write layer can
# grow a sparse VMDK even when guest filesystem usage does not grow.
for service in fe be; do
  name="snow-lab-analysis-doris-$service-1"
  mount=$(sudo docker inspect "$name" --format '{{range .Mounts}}{{if eq .Destination "/snow/'"$service"'.conf"}}{{.Source}}{{end}}{{end}}')
  test "$mount" = "$PWD/lab/doris/$service-realtime.conf" || {
    echo 'Provision the realtime Doris overlay separately with a 3 GiB disk reservation before using this script' >&2; exit 1;
  }
done
for service in fe be; do
  name="snow-lab-analysis-doris-$service-1"
  memory=1280m; cpus=1
  if test "$service" = be; then memory=1792m; cpus=2; fi
  sudo docker update --memory "$memory" --cpus "$cpus" "$name" >/dev/null </dev/null
  sudo docker start "$name" >/dev/null </dev/null
done
sudo docker compose --env-file lab/locks/images.env --env-file lab/.env --env-file lab/secrets/realtime.env \
  -f lab/compose.realtime.yaml up -d kafka jobmanager taskmanager </dev/null
