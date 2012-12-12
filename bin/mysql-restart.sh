#!/bin/sh

command -v mysql.server >/dev/null 2>&1 || { echo >&2 "mysql.server is required to run $0."; exit 1; }

mysql.server stop 
mysql.server start --character-set-server=utf8 --collation-server=utf8_bin --plugin-load=cloud=ha_cloud.so --default-storage-engine=cloud --debug=d:t:L:F:o,/tmp/mysqld.trace

