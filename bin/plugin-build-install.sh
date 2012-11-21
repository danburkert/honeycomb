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
  platform=$(uname)
  if [[ "$platform" == "Darwin" ]]; then
    echo "Creating mysql Mac User"
    sudo dscl . -create /Groups/mysql
    sudo dscl . -create /Groups/mysql gid 296
    sudo dscl . -create /Users/mysql
    sudo dscl . -create /Users/mysql UserShell /usr/bin/false
    sudo dscl . -create /Users/mysql NFSHomeDirectory /var/empty
    sudo dscl . -append /Groups/mysql GroupMembership mysql
  elif [[ "$platform" == "Linux" ]]; then
    echo "Creating mysql Linux User"
    sudo groupadd mysql
    sudo useradd -r -g mysql mysql
  fi

  sudo chown -R mysql $MYSQL_HOME/data
  sudo chgrp -R mysql $MYSQL_HOME/data
  echo "Creating grant tables"
  pushd $MYSQL_HOME
  sudo scripts/mysql_install_db --user=mysql
  echo "Starting up MySQL"
  sudo support-files/mysql.server start
  popd
fi

link=$MYSQL_HOME/lib/plugin/ha_cloud.so 
target=$HONEYCOMB_HOME/build/storage/cloud/ha_cloud.so
if [ ! -h $link ]
then
  if [ -e $link ]; then
    echo "Changing file to symbolic link"
    sudo rm $link
  fi

  echo "Creating a symbolic link from $link to $target"
  sudo ln -s $target $link
fi
