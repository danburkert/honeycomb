#!/bin/bash

: ${MYSQL_HOME?"Need to set MYSQL_HOME environment variable to the MySQL installation location."}

dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# hbase_processes=`$dir/hbase-processes.sh | wc -l`
# [ $hbase_processes -eq 3 ] || { echo >&2 "HBase is not running or not running correctly. Only $hbase_processes processes found."; exit 1; }

MYSQL_STARTUP_SCRIPT=$MYSQL_HOME/support-files/mysql.server

if [ ! -x $MYSQL_STARTUP_SCRIPT ]
then
    echo >&2 "$MYSQL_STARTUP_SCRIPT must be available and executable to run $0."
    exit 1
fi

$MYSQL_STARTUP_SCRIPT stop
$MYSQL_STARTUP_SCRIPT start --character-set-server=utf8 --collation-server=utf8_bin --plugin-load=Honeycomb=ha_honeycomb.so --default-storage-engine=Honeycomb
