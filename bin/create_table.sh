DROP_DB="DROP DATABASE hbase;"
CREATE_DB="CREATE DATABASE hbase;"
USE_DB="USE hbase;"
CREATE_TABLE="CREATE TABLE foo (bar int) ENGINE=cloud;"

echo $DROP_DB > create_table.sql
echo $CREATE_DB >> create_table.sql
echo $USE_DB >> create_table.sql
echo $CREATE_TABLE >> create_table.sql

mysql -u root < create_table.sql

rm create_table.sql
