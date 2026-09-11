#!/usr/bin/env bash
set -euo pipefail
# For new dedicated Snow Statistics Ubuntu guests only.
case "$(hostname)" in snow-control|snow-compute|snow-analysis) ;; *) echo 'Unexpected guest' >&2; exit 1;; esac
sudo apt-get update -qq
sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends docker.io docker-compose-v2 python3-venv curl ca-certificates > /tmp/snow-bootstrap-packages.log
sudo systemctl enable --now docker
sudo docker version --format '{{.Server.Version}}'
