#!/bin/sh

: ${MYSQL_HOME?"Need to set MYSQL_HOME environmental variable."}
cd $MYSQL_HOME/mysql-test
rm suite/cloud-test/r/*.reject
./mtr --suite=cloud-test --extern socket=/tmp/mysql.sock --force --retry=2 --max-test-fail=10
