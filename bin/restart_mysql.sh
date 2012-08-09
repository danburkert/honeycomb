#!/bin/bash
DELETE_PLUGIN="DELETE FROM mysql.plugin WHERE true;"
REMOVE_PLUGIN="UNINSTALL PLUGIN cloud;"
INSTALL_PLUGIN="INSTALL PLUGIN cloud SONAME 'ha_cloud.so';"

echo $DELETE_PLUGIN > remove_plugin.sql
echo $REMOVE_PLUGIN >> remove_plugin.sql
echo $INSTALL_PLUGIN > install_plugin.sql

mysql -u root < remove_plugin.sql
mysql.server stop
rm /usr/local/Cellar/mysql/5.5.25a/lib/plugin/ha_cloud.so
rm /usr/local/Cellar/mysql/5.5.25a/lib/plugin/mysqlengine-0.1-jar-with-dependencies.jar

mysql.server start --debug=d:t:o:F:L,/tmp/mysqld.trace

ln -s /Users/dburkert/NIC/mysql-cloud-engine/build/storage/cloud/ha_cloud.so /usr/local/Cellar/mysql/5.5.25a/lib/plugin
ln -s /Users/dburkert/NIC/mysql-cloud-engine/HBaseAdapter/target/mysqlengine-0.1-jar-with-dependencies.jar /usr/local/Cellar/mysql/5.5.25a/lib/plugin/

mysql -u root < install_plugin.sql

rm remove_plugin.sql
rm install_plugin.sql
