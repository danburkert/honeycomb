#!/bin/sh

: ${MYSQL_HOME?"Need to set MYSQL_HOME environmental variable."; exit 1; }
: ${HONEYCOMB_HOME?"Need to set HONEYCOMB_HOME environmental variable."; exit 1; }

test_link=$MYSQL_HOME/mysql-test/suite/honeycomb-test
source=$HONEYCOMB_HOME/honeycomb/honeycomb-test
if [ ! -e $source ]
then
  echo "Could not find $source path. Not creating a link."
  exit 1
fi

if [ -L $test_link ] || [ -d $test_link ]
then
  echo "$test_link already exists. Not creating a link."
  exit 0
fi 

if [ ! -L $test_link ]
then
  echo "Creating a link from $source $test_link"
  ln -s $source $test_link
fi
