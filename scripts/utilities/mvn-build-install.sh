#!/bin/bash

: ${HONEYCOMB_HOME?"Need to set HONEYCOMB_HOME environmental variable to the top of the project."}
command -v mvn >/dev/null 2>&1 || { echo >&2 "mvn is required to run $0."; exit 1; }

if [ ! -z "$HONEYCOMB_LIB" ]
then
  honeycomb_lib=$HONEYCOMB_LIB
else
  honeycomb_lib=/usr/local/lib/honeycomb
fi

script_dir=$HONEYCOMB_HOME/scripts/utilities
source $script_dir/constants.sh
echo -e "Running Maven build script\n"

testOption=$1
mvnTestMode="-DskipIntTests"

if [ -n "$testOption" ]
then
    case "$testOption" in
        none)
            echo -e "Disabling unit and integration tests\n"
            mvnTestMode="-DskipTests"
            ;;
        all)
            echo -e "Enabling unit and integration tests\n"
            mvnTestMode=""
            ;;
        it)
            echo -e "Disabling unit tests\n"
            mvnTestMode="-DskipUnitTests"
            ;;
        ut)
            echo -e "Disabling integration tests\n"
            mvnTestMode="-DskipIntTests"
            ;;
        *)
            echo -e "Unknown Maven test mode specified ($testOption), running unit tests\n"
            ;;
    esac
else
    echo "Test running mode not specified"
fi



cd $HONEYCOMB_HOME

mvn -V clean install -Dapache $mvnTestMode
[ $? -ne 0 ] && { exit 1; }

$script_dir/install-honeycomb-jars.sh "$HONEYCOMB_HOME/storage-engine-backends/hbase" $honeycomb_lib

adapter_conf=$CONFIG_PATH/honeycomb.xml
if [ ! -d $CONFIG_PATH ]
then
  echo "Creating configuration path $config_path"
  sudo mkdir $CONFIG_PATH
fi

if [ ! -e $adapter_conf ]
then
  echo "Creating the honeycomb.xml from the repository."
  sudo cp $HONEYCOMB_HOME/config/honeycomb.xml $adapter_conf
fi

jar=mysqlengine-0.1.jar
classpath=$HONEYCOMB_HOME/storage-engine-backends/hbase/target/classpath
if [ "$($script_dir/check-honeycomb-xml.rb "$classpath" $jar)" == "Update" ]
then
  echo "Updating honeycomb.xml, it's out of date"
  sudo $script_dir/update-honeycomb-xml.rb "$classpath" $jar
fi

echo "*** Don't forget to restart MySQL. The JVM doesn't autoreload the jar from the disk. ***"
