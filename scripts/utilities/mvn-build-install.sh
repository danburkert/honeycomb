#!/bin/bash

: ${HONEYCOMB_SOURCE?"Need to set HONEYCOMB_SOURCE environmental variable to the top of the project."}
command -v mvn >/dev/null 2>&1 || { echo >&2 "mvn is required to run $0."; exit 1; }
script_dir=$HONEYCOMB_SOURCE/scripts/utilities
source $script_dir/constants.sh

function install_artifact_jar
{
  src=$1

  echo "Copying jars into $honeycomb_lib"
  cp -v $src/target/*-jar-with-dependencies.jar $honeycomb_lib
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
            echo -e "Enabling integration tests\n"
            mvnTestMode="-DskipUnitTests"
            ;;
        ut)
            echo -e "Enabling unit tests\n"
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

# Create the directory used to store the project artifacts, if needed
create_dir_with_ownership $honeycomb_lib

# Remove all existing jars
if [[ -n `find $honeycomb_lib -name *.jar` ]]
then
  echo "Deleting all existing jars..."
  rm -fv $honeycomb_lib/*.jar
fi

# Install current project artifacts
install_artifact_jar "$HBASE_BACKEND"
install_artifact_jar "$MEMORY_BACKEND"
install_artifact_jar "$PROXY"

echo "*** Don't forget to restart MySQL. The JVM doesn't auto-reload the jars from the disk. ***"
