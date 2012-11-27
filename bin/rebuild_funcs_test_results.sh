#!/bin/sh
: ${MYSQL_HOME?"Need to set MYSQL_HOME environmental variable."}

# Clear out old results
rm $MYSQL_HOME/mysql-test/suite/cloud-test/r/*

# Record baseline results with InnoDB
cd $MYSQL_HOME/mysql-test
./mtr --suite=cloud-test/funcs             \
  --mysqld=--default-storage-engine=InnoDB \
  --mysqld=--character-set-server=utf8     \
  --mysqld=--collation-server=utf8_bin     \
  --record                                 \
ai_init_insert

# Move test results that are manually built
#cp $MYSQL_HOME/mysql-test/suite/cloud-test/t/manual_results/* \
#   $MYSQL_HOME/mysql-test/suite/cloud-test/r/
