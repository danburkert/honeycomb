#!/bin/sh
: ${MYSQL_HOME?"Need to set MYSQL_HOME environmental variable."}

# Clear out old results
rm $MYSQL_HOME/mysql-test/suite/honeycomb-test/iuds/r/*

# Record baseline results with InnoDB
cd $MYSQL_HOME/mysql-test
./mtr --suite=honeycomb-test/iuds              \
  --mysqld=--default-storage-engine=InnoDB \
  --mysqld=--character-set-server=utf8     \
  --mysqld=--collation-server=utf8_bin     \
  --record                                 \
delete_decimal \
delete_time \
delete_year \
insert_calendar \
insert_decimal \
insert_number \
insert_time \
insert_year \
strings_charsets_update_delete \
strings_update_delete \
type_bit_iuds \
update_decimal \
update_delete_calendar \
update_delete_number \
update_time \
update_year \

# Move test results that are manually built
#cp $MYSQL_HOME/mysql-test/suite/honeycomb-test/t/manual_results/* \
#   $MYSQL_HOME/mysql-test/suite/honeycomb-test/r/
