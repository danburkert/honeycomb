#!/bin/bash

: ${HONEYCOMB_HOME?"Need to set HONEYCOMB_HOME environmental variable to the top of the project."}
: ${MYSQL_HOME?"Need to set MYSQL_HOME environmental variable to MySQL's installation directory."}
command -v mvn >/dev/null 2>&1 || { echo >&2 "mvn is required to run $0."; exit 1; }
command -v cmake >/dev/null 2>&1 || { echo >&2 "cmake is required to run $0."; exit 1; }
command -v make >/dev/null 2>&1 || { echo >&2 "make is required to run $0."; exit 1; }
command -v mysql.server >/dev/null 2>&1 || { echo >&2 "WARNING: mysql.server should be in the path. It is found in $MYSQL_HOME/support-files"; }
command -v mysqld >/dev/null 2>&1 || { echo >&2 "WARNING: mysqld should be in the path. It is found in $MYSQL_HOME/bin"; }
