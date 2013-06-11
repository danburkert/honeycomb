#!/bin/bash

: ${HONEYCOMB_SOURCE?"Need to set HONEYCOMB_SOURCE environmental variable to the top of the project."}
: ${MYSQL_SOURCE?"Need to set MYSQL_SOURCE environmental variable to MySQL's installation directory."}
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

link $STORAGE_ENGINE $honeycomb_link

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

  cmake -DWITH_DEBUG=$with_debug -DMYSQL_MAINTAINER_MODE=0 $mysql_path -DCMAKE_INSTALL_PREFIX=$MYSQL_HOME
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

cd $BUILD_DIR

# If the MySQL home directory doesn't exist or is empty, build and install to it
if [ ! -d $MYSQL_HOME -o -z "$(ls -A $MYSQL_HOME)" ]
then
  echo "Installing and Configuring MySQL."
  sudo make install

  echo "Changing the owner of $MYSQL_HOME to $current_user:$current_group"
  take_ownership $MYSQL_HOME
  echo "Creating grant tables"
  pushd $MYSQL_HOME
  scripts/mysql_install_db --user=$current_user
  [ $? -ne 0 ] && { echo "mysql_install_db failed.  Exiting build."; exit 1; }
  popd
fi

link $BUILD_OUTPUT/$SO_NAME $MYSQL_HOME/lib/plugin/$SO_NAME
