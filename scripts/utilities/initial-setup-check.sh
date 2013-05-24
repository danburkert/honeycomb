#!/bin/bash

env_correct=true
if [ -z "$HONEYCOMB_SOURCE" ]
then
    echo "Need to set HONEYCOMB_SOURCE environmental variable to the root of the honeycomb source directory."
    env_correct=false
fi

if [ -z "$MYSQL_HOME" ]
then
    echo "Need to set MYSQL_HOME environmental variable to MySQL's installation directory."
    env_correct=false
fi

if [ -z "$MYSQL_SOURCE" ]
then
    echo "Need to set MYSQL_SOURCE environmental variable to MySQL's source directory."
    env_correct=false
fi

if ! $env_correct
then
    exit 1
fi


command -v mvn >/dev/null 2>&1 || { echo >&2 "mvn is required to run $0."; exit 1; }
command -v cmake >/dev/null 2>&1 || { echo >&2 "cmake is required to run $0."; exit 1; }
command -v make >/dev/null 2>&1 || { echo >&2 "make is required to run $0."; exit 1; }
command -v mysql.server >/dev/null 2>&1 || { echo >&2 "WARNING: mysql.server should be in the path. It is found in $MYSQL_HOME/support-files"; }
command -v mysqld >/dev/null 2>&1 || { echo >&2 "WARNING: mysqld should be in the path. It is found in $MYSQL_HOME/bin"; }


source $HONEYCOMB_SOURCE/scripts/utilities/constants.sh

# Create the directory required for application logging, if it doesn't exist
create_dir_with_ownership $APP_LOGGING_PATH