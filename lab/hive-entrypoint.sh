#!/usr/bin/env bash
set -euo pipefail
export HIVE_CONF_DIR=/opt/hive/conf HADOOP_CONF_DIR=/opt/hive/conf
export HADOOP_CLIENT_OPTS="-Xmx512m"
for file in /hive_custom_conf/*.xml; do ln -sfn "$file" "$HIVE_CONF_DIR/$(basename "$file")"; done
if test -d /opt/hive/data/metastore_db; then
  /opt/hive/bin/schematool -dbType derby -validate
else
  /opt/hive/bin/schematool -dbType derby -initSchema
fi
exec /opt/hive/bin/hive --skiphadoopversion --skiphbasecp --service metastore
