#!/bin/bash

: ${HONEYCOMB_HOME?"Need to set HONEYCOMB_HOME environmental variable to the top of the project."}
command -v mvn >/dev/null 2>&1 || { echo >&2 "mvn is required to run $0."; exit 1; }

if [ $# -eq 1 ]
then
  honeycomb_lib=$1
elif [ ! -z "$HONEYCOMB_LIB" ]
then
  honeycomb_lib=$HONEYCOMB_LIB
else
  honeycomb_lib=/usr/local/lib/honeycomb
fi

cd $HONEYCOMB_HOME
echo "Running: mvn install"
mvn package install -Dapache
$HONEYCOMB_HOME/bin/install-honeycomb-jars.sh "$HONEYCOMB_HOME/HBaseAdapter" $honeycomb_lib

conf_path=/etc/mysql
adapter_conf=$conf_path/honeycomb.xml
if [ ! -d $conf_path ]
then
  echo "Creating configuration path $conf_path"
  sudo mkdir $conf_path
fi

if [ ! -e $adapter_conf ]
then
  echo "Creating the honeycomb.xml from the repository."
  sudo cp $HONEYCOMB_HOME/honeycomb/honeycomb-example.xml $adapter_conf
fi
sudo $HONEYCOMB_HOME/bin/update-honeycomb-xml.rb "$HONEYCOMB_HOME" mysqlengine-0.1.jar
echo "*** Don't forget to restart MySQL. The JVM doesn't autoreload the jar from the disk. ***"
