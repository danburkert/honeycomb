#!/bin/sh

honeycomb_lib=/usr/local/lib/honeycomb
plugin_path=/usr/lib64/mysql/plugin/ha_cloud.so
tests_path=/usr/share/mysql-test/suite/cloud-test
function test_link
{
  path=$1
  [ -L $path ] || { echo "Link $path is missing. Can't continue installing."; exit 1; }
}

[ -d $honeycomb_lib ] && [ -w $honeycomb_lib ] || { echo "$honeycomb_lib is required, and has to be writable."; exit 1; }
test_link $plugin_path
test_link $tests_path

if [[ "$(find $honeycomb_lib -type d -empty)" == "" ]]
then
  echo "Removing old code from $honeycomb_lib"
  rm -rf $honeycomb_lib/*
fi

echo "Copying ha_cloud.so, mysqlengine.jar and cloud-test to $honeycomb_lib"
cp -R $HONEYCOMB_HOME/mysql-5.5.28/storage/cloud/cloud-test $honeycomb_lib
cp $HONEYCOMB_HOME/build/storage/cloud/ha_cloud.so $honeycomb_lib
function copy_jar
{
  cp $HONEYCOMB_HOME/$1/target/*.jar $honeycomb_lib
}

copy_jar HBaseAdapter
copy_jar MedianSplit
copy_jar DataCreator
copy_jar BulkLoadMapper

create_classpath=$($HONEYCOMB_HOME/bin/create-classpath.rb $honeycomb_lib)
echo $create_classpath > /home/teamcity/classpath.conf
echo "Running mysql-restart.sh"
$SETUID/mysql-restart.sh
