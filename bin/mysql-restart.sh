#!/bin/sh
REMOVE_PLUGIN="UNINSTALL PLUGIN cloud;"
INSTALL_PLUGIN="INSTALL PLUGIN cloud SONAME 'ha_cloud.so';"

echo $REMOVE_PLUGIN > remove_plugin.sql
echo $INSTALL_PLUGIN > install_plugin.sql

sudo mysql.server stop && sudo mysql.server start --debug=d:t:L:F:o,/tmp/mysqld.trace

echo "removing plugin"
mysql -u root < remove_plugin.sql
echo "installing plugin"
mysql -u root < install_plugin.sql

rm remove_plugin.sql
rm install_plugin.sql
