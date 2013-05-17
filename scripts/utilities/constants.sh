#!/bin/sh

CONFIG_PATH=/usr/share/mysql/honeycomb
DEFAULT_HONEYCOMB_LIB=/usr/local/lib/honeycomb

BACKENDS=$HONEYCOMB_HOME/storage-engine-backends
HBASE_BACKEND=$BACKENDS/hbase
MEMORY_BACKEND=$BACKENDS/memory
STORAGE_ENGINE=$HONEYCOMB_HOME/storage-engine
HONEYCOMB_CONFIG=$HONEYCOMB_HOME/config
BUILD_DIR=$HONEYCOMB_HOME/build
BUILD_OUTPUT=$BUILD_DIR/storage/honeycomb

SO_NAME=ha_honeycomb.so
SCHEMA_NAME=honeycomb.xsd
CONFIG_NAME=honeycomb.xml

function take_dir 
{
  dir=$1
  if [ ! -d $dir ]
  then
    echo "Creating directory: $dir"
    mkdir $dir
  fi

  cd $dir
}

function link
{
  target=$1
  link=$2
  admin=false
  if [ $# -eq 3 ]
  then
    admin=true
  fi

  if [ ! -h $link ]
  then
    if [ -e $link ]; then
      echo "Changing file to symbolic link"
      rm $link
    fi

    echo "Creating a symbolic link from $target to $link "
    if $admin
    then
      ln -s $target $link
    else
      sudo ln -s $target $link
    fi
  fi
}
