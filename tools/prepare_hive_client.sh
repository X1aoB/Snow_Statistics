#!/usr/bin/env bash
set -euo pipefail
cd /home/snow/Snow_Statistics
set -a
. lab/locks/images.env
set +a
mkdir -p runtime/hive-client
# Dependencies come from the pinned Hive image, with a checksum inventory for replay.
sudo docker run --rm --user 0:0 --entrypoint bash -v "$PWD/runtime/hive-client:/out" "$HIVE_IMAGE" -ec '
  cp -n /opt/hive/lib/*.jar /out/
  cp -n /opt/hadoop/share/hadoop/common/*.jar /out/
  cp -n /opt/hadoop/share/hadoop/common/lib/*.jar /out/
  cp -n /opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-*.jar /out/
'
(cd runtime/hive-client && sha256sum --status -c ../../lab/locks/hive-client.sha256)
test "$(find runtime/hive-client -maxdepth 1 -name '*.jar' | wc -l)" -eq "$(wc -l < lab/locks/hive-client.sha256)"
du -sh runtime/hive-client
