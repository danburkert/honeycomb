#!/bin/sh

command -v mysql.server >/dev/null 2>&1 || { echo >&2 "mysql.server is required to run $0."; exit 1; }
dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
hbase_processes=`$dir/hbase-processes.sh | wc -l`
[ $hbase_processes -eq 3 ] || { echo >&2 "HBase is not running or not running correctly. Only $hbase_processes processes found."; exit 1; }

mysql.server stop
mysql.server start --character-set-server=utf8 --collation-server=utf8_bin --plugin-load=Honeycomb=ha_honeycomb.so --default-storage-engine=Honeycomb --debug=d:t:i:L:F:o,/tmp/mysqld.trace

