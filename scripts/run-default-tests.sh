#!/bin/sh

: ${MYSQL_HOME?"Need to set MYSQL_HOME environmental variable."; exit 1; }

cd $MYSQL_HOME/mysql-test
rm suite/honeycomb-test/default/r/*.reject
./mtr --suite=honeycomb-test/default           \
  --mysqld=--plugin-load=Honeycomb=ha_honeycomb.so \
  --mysqld=--default-storage-engine=Honeycomb  \
  --mysqld=--character-set-server=utf8     \
  --mysqld=--collation-server=utf8_bin     \
  --force --retry=0 --max-test-fail=10
