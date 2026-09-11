#!/usr/bin/env bash
set -euo pipefail
test "$(hostname)" = snow-control
cd /home/snow/Snow_Statistics
sudo install -d -o 50000 -g 0 -m 2770 lab/secrets/airflow-ssh
sudo install -d -o 50000 -g 1000 -m 2770 runtime/publication
if ! sudo test -f lab/secrets/airflow-ssh/id_ed25519; then
  sudo ssh-keygen -q -t ed25519 -N '' -C snow-statistics-airflow -f lab/secrets/airflow-ssh/id_ed25519
fi
set -a
. lab/.env
set +a
sudo sh -c 'printf "%s " "$1"; cat /etc/ssh/ssh_host_ed25519_key.pub' sh "$CONTROL_IP" | sudo tee lab/secrets/airflow-ssh/known_hosts >/dev/null
public_key=$(sudo cat lab/secrets/airflow-ssh/id_ed25519.pub)
mkdir -p ~/.ssh
touch ~/.ssh/authorized_keys
if ! grep -qF "$public_key" ~/.ssh/authorized_keys; then
  printf 'restrict,command="/usr/bin/python3 /home/snow/Snow_Statistics/tools/airflow_gateway.py" %s\n' "$public_key" >> ~/.ssh/authorized_keys
fi
chmod 700 ~/.ssh
chmod 600 ~/.ssh/authorized_keys
sudo chown 50000:0 lab/secrets/airflow-ssh/id_ed25519 lab/secrets/airflow-ssh/id_ed25519.pub lab/secrets/airflow-ssh/known_hosts
sudo chmod 600 lab/secrets/airflow-ssh/id_ed25519
echo 'Restricted Airflow gateway and seeded host trust ready (key values hidden)'
