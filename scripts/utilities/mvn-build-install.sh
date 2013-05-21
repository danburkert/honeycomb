#!/bin/bash

: ${HONEYCOMB_SOURCE?"Need to set HONEYCOMB_SOURCE environmental variable to the top of the project."}
command -v mvn >/dev/null 2>&1 || { echo >&2 "mvn is required to run $0."; exit 1; }
script_dir=$HONEYCOMB_SOURCE/scripts/utilities
source $script_dir/constants.sh

function install_jars
{
  src=$1
  lib=$2
  create_dir_with_ownership $lib

  echo "Moving jars into $lib"
  cp $src/target/*-$ARTIFACT_ID-jar-with-dependencies.jar $lib
}

if [ ! -z "$HONEYCOMB_LIB" ]
then
  honeycomb_lib=$HONEYCOMB_LIB
else
  honeycomb_lib=$DEFAULT_HONEYCOMB_LIB
fi
echo -e "Running Maven build script\n"

testOption=$1
mvnTestMode="-DskipIntTests"
adapter_conf=$CONFIG_PATH/$CONFIG_NAME

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

cd $HONEYCOMB_SOURCE

mvn -V clean install $mvnTestMode
[ $? -ne 0 ] && { exit 1; }

install_jars "$HBASE_BACKEND" $honeycomb_lib
install_jars "$MEMORY_BACKEND" $honeycomb_lib
install_jars "$PROXY" $honeycomb_lib

create_dir_with_ownership $CONFIG_PATH

if [ ! -e $adapter_conf ]
then
  echo "Creating the honeycomb.xml from the repository."
  sudo cp $HONEYCOMB_CONFIG/$CONFIG_NAME $adapter_conf
  take_ownership $adapter_conf
fi

echo "*** Don't forget to restart MySQL. The JVM doesn't autoreload the jar from the disk. ***"
