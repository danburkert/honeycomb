#!/bin/bash

: ${HONEYCOMB_HOME?"Need to set HONEYCOMB_HOME environmental variable to the top of the project."}
: ${MYSQL_HOME?"Need to set MYSQL_HOME environmental variable to MySQL's installation directory."}
command -v cmake >/dev/null 2>&1 || { echo >&2 "cmake is required to run $0."; exit 1; }
command -v make >/dev/null 2>&1 || { echo >&2 "make is required to run $0."; exit 1; }

build_dir=$HONEYCOMB_HOME/build
unit_test_dir=$HONEYCOMB_HOME/build/storage/honeycomb/unit-test

if [ ! -L $HONEYCOMB_HOME/mysql ]
then
    echo "Create a symbolic link to your mysql source code in $HONEYCOMB_HOME (ln -s <mysql source directory> mysql)"
    exit 1
fi

honeycomb_link=$HONEYCOMB_HOME/mysql/storage/honeycomb
if [ ! -L $honeycomb_link ]
then
    echo "Creating symbolic link to $honeycomb_link"
    ln -s $HONEYCOMB_HOME/honeycomb $honeycomb_link
fi

if [ ! -d $build_dir ]
then
  echo "Creating build output directory: $build_dir"
  mkdir $build_dir
fi

cd $build_dir

if [ ! -e CMakeCache.txt ]
then
  echo "Running cmake with debug enabled."
  cmake -DWITH_DEBUG=1 -DMYSQL_MAINTAINER_MODE=0 ../mysql
  [ $? -ne 0 ] && { echo "CMake failed stopping the script.\n*** Don't forget to delete CMakeCache.txt before running again.***"; exit 1; }
fi

echo "Running make in $build_dir"
make
[ $? -ne 0 ] && { echo "Make failed stopping the script."; exit 1; }

if [ ! -d $unit_test_dir ]
then
  echo "Creating test build directory: $unit_test_dir"
  mkdir $unit_test_dir
fi

cd $unit_test_dir
if [ ! -e CMakeCache.txt ]
then
  cmake $HONEYCOMB_HOME/honeycomb/unit-test -DHONEYCOMB_SOURCE_DIR=$HONEYCOMB_HOME/honeycomb
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

link=$MYSQL_HOME/lib/plugin/ha_honeycomb.so
target=$build_dir/storage/honeycomb/ha_honeycomb.so
if [ ! -h $link ]
then
  if [ -e $link ]; then
    echo "Changing file to symbolic link"
    rm $link
  fi

  echo "Creating a symbolic link from $link to $target"
  ln -s $target $link
fi

link=/etc/mysql/honeycomb.xsd
target=$HONEYCOMB_HOME/honeycomb/honeycomb.xsd
if [ ! -h $link ]
then
  echo "Creating a symbolic link from $link to $target"
  sudo ln -s $target $link
fi
