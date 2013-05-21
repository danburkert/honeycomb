#!/bin/bash

: ${HONEYCOMB_SOURCE?"Need to set HONEYCOMB_SOURCE environmental variable to the top of the project."}
: ${MYSQL_SOURCE?"Need to set MYSQL_HOME environmental variable to MySQL's installation directory."}
command -v cmake >/dev/null 2>&1 || { echo >&2 "cmake is required to run $0."; exit 1; }
command -v make >/dev/null 2>&1 || { echo >&2 "make is required to run $0."; exit 1; }
source $HONEYCOMB_SOURCE/scripts/utilities/constants.sh

if [ $# -eq 1 ]
then
    mysql_path=$1
else
    : ${MYSQL_SOURCE?"Need to set MYSQL_SOURCE if you want to run this script without arguments."}
    mysql_path=$MYSQL_SOURCE
    echo $mysql_path
fi

unit_test_dir=$BUILD_OUTPUT/unit-test
honeycomb_link=$mysql_path/storage/honeycomb

take_dir $BUILD_DIR

if [ ! -e CMakeCache.txt ]
then
  if $DEV_MODE; then
    with_debug=1;
    echo "Running CMake with debug enabled."
  else
    with_debug=0;
    echo "Running CMake."
  fi

  cmake -DWITH_DEBUG=$with_debug -DMYSQL_MAINTAINER_MODE=0 $mysql_path
  [ $? -ne 0 ] && { echo "Failure during CMake step.  Exiting build.";
                    rm CMakeCache.txt;
                    exit 1; }
fi

echo "Running make in $BUILD_DIR"
make
[ $? -ne 0 ] && { echo "Failure during make step.  Exiting build."; exit 1; }

take_dir $unit_test_dir

if [ ! -e CMakeCache.txt ]
then
  cmake $STORAGE_ENGINE/unit-test -DHONEYCOMB_SOURCE_DIR=$STORAGE_ENGINE
  [ $? -ne 0 ] && { "Failure during CMake step on unit tests.  Exiting build.";
                    rm CMakeCache.txt;
                    exit 1; }
fi
make
[ $? -ne 0 ] && { "Failure during make step on unit tests.  Exiting build."; exit 1; }
echo "Running Honeycomb unit tests"
make test
[ $? -ne 0 ] && { echo "Unit test failed. Exiting Build. Execute build/storage/honeycomb/unit-test/runUnitTests for more details."; exit 1; }

if [ ! -d $MYSQL_HOME ]
then
  echo "Installing and Configuring MySQL."
  sudo make install
  current_user=`whoami`
  current_group=`groups | awk '{ print $1 }'`

  echo "Changing the owner of $MYSQL_HOME to $current_user:$current_group"
  sudo chown -R $current_user:$current_group $MYSQL_HOME
  echo "Creating grant tables"
  pushd $MYSQL_HOME
  scripts/mysql_install_db --user=$current_user
  [ $? -ne 0 ] && { echo "mysql_install_db failed.  Exiting build."; exit 1; }
  echo "Starting MySQL"
  support-files/mysql.server start
  [ $? -ne 0 ] && { echo "Starting MySQL server failed.  Exiting build."; exit 1; }
  popd
fi

link $STORAGE_ENGINE $honeycomb_link
link $BUILD_OUTPUT/$SO_NAME $MYSQL_HOME/lib/plugin/$SO_NAME
link $HONEYCOMB_CONFIG/$SCHEMA_NAME $CONFIG_PATH/$SCHEMA_NAME use_admin
