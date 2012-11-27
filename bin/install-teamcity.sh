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
cp -R $HONEYCOMB_HOME/cloud/cloud-test $honeycomb_lib
cp $HONEYCOMB_HOME/build/storage/cloud/ha_cloud.so $honeycomb_lib
cp $HONEYCOMB_HOME/HBaseAdapter/target/lib/*.jar $honeycomb_lib
cp $HONEYCOMB_HOME/HBaseAdapter/target/mysqlengine-0.1.jar $honeycomb_lib
chmod a+x $honeycomb_lib/*.jar
