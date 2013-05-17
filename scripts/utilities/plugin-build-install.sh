#!/bin/bash

: ${HONEYCOMB_HOME?"Need to set HONEYCOMB_HOME environmental variable to the top of the project."}
: ${MYSQL_HOME?"Need to set MYSQL_HOME environmental variable to MySQL's installation directory."}
command -v cmake >/dev/null 2>&1 || { echo >&2 "cmake is required to run $0."; exit 1; }
command -v make >/dev/null 2>&1 || { echo >&2 "make is required to run $0."; exit 1; }
source $HONEYCOMB_HOME/scripts/utilities/constants.sh

if [ $# -eq 1 ]
then
    mysql_path=$1
else
    : ${MYSQL_SOURCE_PATH?"Need to set MYSQL_SOURCE_PATH if you want to run this script without arguments."}
    mysql_path=$MYSQL_SOURCE_PATH
    echo $mysql_path
fi

unit_test_dir=$BUILD_OUTPUT/unit-test
honeycomb_link=$mysql_path/storage/honeycomb

take_dir $BUILD_DIR

if [ ! -e CMakeCache.txt ]
then
  echo "Running cmake with debug enabled."
  cmake -DWITH_DEBUG=1 -DMYSQL_MAINTAINER_MODE=0 $mysql_path
  [ $? -ne 0 ] && { echo "CMake failed stopping the script.\n*** Don't forget to delete CMakeCache.txt before running again.***"; exit 1; }
fi

echo "Running make in $BUILD_DIR"
make
[ $? -ne 0 ] && { echo "Make failed stopping the script."; exit 1; }

take_dir $unit_test_dir

if [ ! -e CMakeCache.txt ]
then
  cmake $STORAGE_ENGINE/unit-test -DHONEYCOMB_SOURCE_DIR=$STORAGE_ENGINE
  [ $? -ne 0 ] && { "CMake failed on unit tests.\n*** Don't forget to delete CMakeCache.txt in the unit test directory before running again.***"; exit 1; }
fi
make
[ $? -ne 0 ] && { exit 1; }
echo "Running Honeycomb unit tests"
make test
[ $? -ne 0 ] && { echo "Unit test failed. Stopping Build. Execute build/storage/honeycomb/unit-test/runUnitTests for more details."; exit 1; }

if [ ! -d $MYSQL_HOME ]
then
  echo "Installing and setting up mysql."
  sudo make install
  current_user=`whoami`
  current_group=`groups | awk '{ print $1 }'`

  echo "Changing the owner of $MYSQL_HOME to $current_user:$current_group"
  sudo chown -R $current_user:$current_group $MYSQL_HOME
  echo "Creating grant tables"
  pushd $MYSQL_HOME
  scripts/mysql_install_db --user=$current_user
  [ $? -ne 0 ] && { echo "mysql_install_db failed stopping the script."; exit 1; }
  echo "Starting up MySQL"
  support-files/mysql.server start
  [ $? -ne 0 ] && { echo "Starting MySQL server failed, stopping the script."; exit 1; }
  popd
fi

link $STORAGE_ENGINE $honeycomb_link
link $BUILD_OUTPUT/$SO_NAME $MYSQL_HOME/lib/plugin/$SO_NAME
link $HONEYCOMB_CONFIG/$SCHEMA_NAME $CONFIG_PATH/$SCHEMA_NAME use_admin
