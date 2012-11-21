#!/bin/sh

: ${HONEYCOMB_HOME?"Need to set HONEYCOMB_HOME environmental variable to the top of the project."}
command -v mysql >/dev/null 2>&1 || { echo >&2 "mysql is required to run $0."; exit 1; }
command -v mysql.server >/dev/null 2>&1 || { echo >&2 "mysql.server is required to run $0."; exit 1; }

REMOVE_PLUGIN="UNINSTALL PLUGIN cloud;"
INSTALL_PLUGIN="INSTALL PLUGIN cloud SONAME 'ha_cloud.so';"

echo $REMOVE_PLUGIN > remove_plugin.sql
echo $INSTALL_PLUGIN > install_plugin.sql

sudo mysql.server stop 
$HONEYCOMB_HOME/bin/kill-mysqld.sh # Ensure mysql is gone.
sudo mysql.server start --debug=d:t:L:F:o,/tmp/mysqld.trace

echo "removing plugin"
mysql -u root < remove_plugin.sql
echo "installing plugin"
mysql -u root < install_plugin.sql

rm remove_plugin.sql
rm install_plugin.sql
