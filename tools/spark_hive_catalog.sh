#!/usr/bin/env bash
set -euo pipefail
cd /home/snow/Snow_Statistics
test "$#" -eq 2
[[ "$1" =~ ^runtime/real/lifecycle/[a-z][a-z0-9-]{2,23}/hive-requests/[a-f0-9]{64}\.json$ ]]
[[ "$2" =~ ^[a-f0-9]{64}$ ]]
test "$(readlink -f -- "$1")" = "$PWD/$1"
# Metadata RPC is intentionally a separate small phase. Do not silently stop
# other containers to make room, or start more services to satisfy this check.
case "$(hostname)" in snow-analysis|snow-control) ;; *) exit 1 ;; esac
for name in $(sudo docker ps --format '{{.Names}}'); do
  case "$(hostname):$name" in
    snow-control:snow-lab-control-namenode-1|snow-control:snow-lab-control-hive-1) ;;
    *) echo 'Hive catalog phase requires the documented exclusive resource window' >&2; exit 1 ;;
  esac
done
available_kib=$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo)
test "${available_kib:-0}" -ge 786432
# Parse the four allowlisted values as data. Never source an operator config.
SPARK_IMAGE=$(sed -n 's/^SPARK_IMAGE=//p' lab/locks/images.env | tr -d '\r')
CONTROL_IP=$(sed -n 's/^CONTROL_IP=//p' lab/.env | tr -d '\r')
COMPUTE_IP=$(sed -n 's/^COMPUTE_IP=//p' lab/.env | tr -d '\r')
ANALYSIS_IP=$(sed -n 's/^ANALYSIS_IP=//p' lab/.env | tr -d '\r')
[[ "$SPARK_IMAGE" =~ ^apache/spark@sha256:[a-f0-9]{64}$ ]]
for ip in "$CONTROL_IP" "$COMPUTE_IP" "$ANALYSIS_IP"; do [[ "$ip" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; done
(cd runtime/hive-client && sha256sum --status -c ../../lab/locks/hive-client.sha256)
test "$(find runtime/hive-client -maxdepth 1 -name '*.jar' | wc -l)" -eq "$(wc -l < lab/locks/hive-client.sha256)"
name="snow-real-hive-${2:0:20}"
request_sha="$2"
cidfile="${1%.json}.cid"
test ! -e "$cidfile"
finish() {
  status=$?
  trap - EXIT
  if test -s "$cidfile"; then
    id=$(cat "$cidfile")
    if [[ "$id" =~ ^[a-f0-9]{64}$ ]]; then
      actual=$(timeout 10s sudo docker inspect --format '{{index .Config.Labels "snow.hive.request"}}' "$id" 2>/dev/null || true)
      if test "$actual" = "$request_sha"; then
        timeout 20s sudo docker stop -t 10 "$id" >/dev/null 2>&1 || true
        remaining=$(timeout 10s sudo docker inspect --format '{{.State.Running}}' "$id" 2>/dev/null || true)
        if test "$remaining" = true; then
          echo 'Owned Hive reader is still running; admission failed closed' >&2
          status=1
        fi
      elif test -n "$actual"; then
        echo 'Hive container label changed; refusing to stop another owner' >&2
        status=1
      fi
    fi
  fi
  exit "$status"
}
trap finish EXIT
trap 'exit 143' TERM
trap 'exit 130' INT
timeout --signal=TERM --kill-after=10s 240s sudo docker run --rm --pull=never --name "$name" --cidfile "$PWD/$cidfile" --label "snow.hive.request=$2" \
  --memory 768m --cpus 1 --pids-limit 128 --network host --user 0:0 \
  --add-host "snow-control:$CONTROL_IP" --add-host "snow-compute:$COMPUTE_IP" --add-host "snow-analysis:$ANALYSIS_IP" \
  --tmpfs /tmp:rw,nosuid,size=192m --read-only \
  -e HADOOP_CONF_DIR=/etc/hadoop -e PYSPARK_PYTHON=/usr/bin/python3 \
  -v "$PWD/src:/opt/snow/src:ro" -v "$PWD/warehouse/spark:/opt/snow/warehouse/spark:ro" \
  -v "$PWD/lab/spark-submit-locked.sh:/opt/snow/lab/spark-submit-locked.sh:ro" \
  -v "$PWD/lab/locks:/opt/snow/lab/locks:ro" -v "$PWD/runtime/hive-client:/opt/snow/runtime/hive-client:ro" \
  -v "$PWD/$1:/opt/snow/$1:ro" -v "$PWD/lab/generated/hadoop:/etc/hadoop:ro" -w /tmp "$SPARK_IMAGE" \
  bash /opt/snow/lab/spark-submit-locked.sh --master 'local[1]' --driver-memory 512m \
  --conf spark.ui.enabled=false --conf spark.eventLog.enabled=false --conf spark.sql.shuffle.partitions=1 \
  --conf spark.sql.hive.metastore.version=3.1.3 --conf spark.sql.hive.metastore.jars=path \
  --conf 'spark.sql.hive.metastore.jars.path=file:///opt/snow/runtime/hive-client/*.jar' \
  /opt/snow/warehouse/spark/real_hive_catalog.py --request "/opt/snow/$1" --sha256 "$2"
