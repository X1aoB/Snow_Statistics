#!/usr/bin/env bash
set -euo pipefail
echo 'Replaced by observed-leader acceptance; see docs/ha.md.'
echo 'On the idle analysis VM: bash tools/start_ha_node.sh kafka-ha'
echo 'Then on the host: uv run --extra lab python tools/smoke_kafka_ha.py --lane NEW_LANE'
echo 'The host tool stops HA containers; explicitly stop the VM afterwards. All histories remain.'
exit 2
