#!/bin/sh

INIT=/opt/flink/conf/init-catalogs.sql

for f in /tmp/scripts_v2/*.sql; do
  echo "Submitting $f"
  ./bin/sql-client.sh --init "$INIT" -f "$f"
done