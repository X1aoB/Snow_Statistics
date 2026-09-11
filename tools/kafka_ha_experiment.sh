#!/usr/bin/env bash
set -euo pipefail
# Dedicated lab network only; production and daily Kafka are not addressed.
dc=(sudo docker compose --env-file lab/locks/images.env -f lab/compose.ha.json --profile kafka-ha)
"${dc[@]}" up -d --wait
"${dc[@]}" exec -T broker3 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker3:9092 \
  --create --if-not-exists --topic snow-ha-proof --partitions 1 --replication-factor 3 --config min.insync.replicas=2
printf 'before-failure\n' | "${dc[@]}" exec -T broker3 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server broker3:9092 --topic snow-ha-proof --producer-property acks=all
"${dc[@]}" stop broker1
printf 'one-node-down\n' | "${dc[@]}" exec -T broker3 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server broker3:9092 --topic snow-ha-proof --producer-property acks=all
"${dc[@]}" exec -T broker3 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker3:9092 --describe --topic snow-ha-proof
# Restore before any two-node-loss exercise; inspect ISR=3. Each further fault is
# explicit so a failed command cannot leave an unattended majority-loss experiment.
"${dc[@]}" start broker1
echo 'Restored broker1. Verify ISR and consume both markers before recording success.'
