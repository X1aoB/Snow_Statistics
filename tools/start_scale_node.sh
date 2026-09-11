#!/usr/bin/env bash
set -euo pipefail
cd /home/snow/Snow_Statistics
test -z "$(sudo docker ps -q)"
set -a
. lab/locks/images.env
. lab/.env
set +a
python3 tools/render_hadoop.py --control "$CONTROL_IP" --compute "$COMPUTE_IP" --scale-small
case "$(hostname)" in
  snow-control) role=control; services=(namenode resourcemanager); overlay=lab/compose.control-scale.yaml ;;
  snow-compute) role=compute; services=(datanode nodemanager); overlay=lab/compose.compute-scale.yaml ;;
  snow-analysis) role=analysis; services=(datanode); overlay=lab/compose.ods-small.yaml ;;
  *) exit 1 ;;
esac
# Images were pinned and installed in prior acceptance. No pull/build in this phase.
sudo docker compose --env-file lab/locks/images.env --env-file lab/.env \
  -f "lab/compose.$role.yaml" -f "$overlay" up -d --no-build --pull never "${services[@]}" </dev/null
