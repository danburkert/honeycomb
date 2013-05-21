#!/bin/bash

: ${MYSQL_HOME?"Need to set MYSQL_HOME environmental variable."; exit 1; }
: ${HONEYCOMB_SOURCE?"Need to set HONEYCOMB_SOURCE environmental variable."; exit 1; }

source $HONEYCOMB_SOURCE/scripts/utilities/constants.sh

test_link=$MYSQL_HOME/mysql-test/suite/honeycomb-test
source=$STORAGE_ENGINE/mysql-test-suites

link $source $test_link
