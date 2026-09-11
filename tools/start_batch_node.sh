#!/usr/bin/env bash
set -euo pipefail
cd /home/snow/Snow_Statistics
set -a
. lab/locks/images.env
. lab/.env
set +a
python3 tools/render_hadoop.py --control "$CONTROL_IP" --compute "$COMPUTE_IP"
case "$(hostname)" in
  snow-control) role=control ;;
  snow-compute) role=compute ;;
  snow-analysis) role=analysis ;;
  *) echo 'Dedicated Snow Statistics guest required' >&2; exit 1 ;;
esac
mkdir -p runtime
for service in $(sudo docker ps --filter "label=com.docker.compose.project=snow-lab-$role" --format '{{.Label "com.docker.compose.service"}}'); do
  case "$role:$service" in control:namenode|control:resourcemanager|control:hive|compute:datanode|compute:nodemanager|analysis:datanode) ;;
    *) echo "Stop $service before starting the batch profile" >&2; exit 1 ;;
  esac
done
sudo docker compose --env-file lab/locks/images.env --env-file lab/.env -f "lab/compose.$role.yaml" --profile batch up -d --build
sudo docker compose --env-file lab/locks/images.env --env-file lab/.env -f "lab/compose.$role.yaml" --profile batch ps
