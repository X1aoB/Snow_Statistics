#!/usr/bin/env bash
set -euo pipefail
case "$(hostname)" in
  snow-control) sudo docker stop snow-lab-control-resourcemanager-1 snow-lab-control-namenode-1 </dev/null ;;
  snow-compute) sudo docker stop snow-lab-compute-nodemanager-1 snow-lab-compute-datanode-1 </dev/null ;;
  snow-analysis) sudo docker stop snow-lab-analysis-datanode-1 </dev/null ;;
  *) exit 1 ;;
esac
