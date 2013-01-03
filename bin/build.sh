#!/bin/sh

: ${HONEYCOMB_HOME?"Need to set HONEYCOMB_HOME environmental variable to the top of the project."}

$HONEYCOMB_HOME/bin/mvn-build-install.sh
$HONEYCOMB_HOME/bin/plugin-build-install.sh
$HONEYCOMB_HOME/bin/mysql-restart.sh
$HONEYCOMB_HOME/bin/install-tests.sh
