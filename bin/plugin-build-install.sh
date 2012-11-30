#!/bin/sh

: ${HONEYCOMB_HOME?"Need to set HONEYCOMB_HOME environmental variable to the top of the project."}
: ${MYSQL_HOME?"Need to set MYSQL_HOME environmental variable to MySQL's installation directory."}
command -v cmake >/dev/null 2>&1 || { echo >&2 "cmake is required to run $0."; exit 1; }
command -v make >/dev/null 2>&1 || { echo >&2 "make is required to run $0."; exit 1; }

build_dir=$HONEYCOMB_HOME/build 
if [ ! -d $build_dir ]
then
  echo "Creating $build_dir"
  mkdir $build_dir
fi

cd $build_dir

if [ ! -e CMakeCache.txt ]
then
  echo "Running cmake with debug enabled."
  cmake -DWITH_DEBUG=1 ../mysql-5.5.28
fi

echo "Running make in $build_dir"
make
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
  echo "Starting up MySQL"
  support-files/mysql.server start
  popd
fi

link=$MYSQL_HOME/lib/plugin/ha_cloud.so
target=$HONEYCOMB_HOME/build/storage/cloud/ha_cloud.so
if [ ! -h $link ]
then
  if [ -e $link ]; then
    echo "Changing file to symbolic link"
    rm $link
  fi

  echo "Creating a symbolic link from $link to $target"
  ln -s $target $link
fi
