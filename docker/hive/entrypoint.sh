#!/bin/sh
set -eu

export HIVE_CONF_DIR=/opt/hive/conf
export SERVICE_NAME=metastore

SCHEMA_FLAG=/opt/hive/data/.schema_initialized

mkdir -p /data/warehouse
mkdir -p /data/warehouse/data

# Trino resolves file:/data/warehouse/... paths inside the shared volume root.
# This compatibility symlink makes Spark-written paths readable from Trino.
if [ ! -e /data/warehouse/data/warehouse ]; then
  ln -s .. /data/warehouse/data/warehouse
fi

if [ "$(id -u)" = "0" ]; then
  chown -R hive:hive /opt/hive/data
  chown -R hive:hive /data/warehouse
  exec su -s /bin/sh hive -c /entrypoint.sh
fi

if [ ! -f "${SCHEMA_FLAG}" ]; then
  /opt/hive/bin/schematool -dbType postgres -initSchema --verbose || \
  /opt/hive/bin/schematool -dbType postgres -upgradeSchema --verbose
  touch "${SCHEMA_FLAG}"
fi

exec /opt/hive/bin/hive --service metastore -p 9083
