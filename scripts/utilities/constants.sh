#!/bin/bash

CONFIG_PATH=/usr/share/mysql/honeycomb
DEFAULT_HONEYCOMB_LIB=/usr/local/lib/honeycomb

APP_LOGGING_PATH=/var/log/mysql

BACKENDS=$HONEYCOMB_SOURCE/storage-engine-backends
PROXY=$HONEYCOMB_SOURCE/storage-engine-proxy
HBASE_BACKEND=$BACKENDS/hbase
MEMORY_BACKEND=$BACKENDS/memory
STORAGE_ENGINE=$HONEYCOMB_SOURCE/storage-engine
HONEYCOMB_CONFIG=$HONEYCOMB_SOURCE/config
BUILD_DIR=$HONEYCOMB_SOURCE/build
BUILD_OUTPUT=$BUILD_DIR/storage/honeycomb

SO_NAME=ha_honeycomb.so
SCHEMA_NAME=honeycomb.xsd
CONFIG_NAME=honeycomb.xml
PROXY_JAR_NAME=honeycomb
HBASE_BACKEND_NAME=honeycomb-hbase
ARTIFACT_ID=0.1-SNAPSHOT

function take_ownership
{
  current_user=`whoami`
  sudo chown -R $current_user $1
}

# Create directory if it doesn't exist and
# change ownership to current user
function create_dir_with_ownership
{
  if [ ! -d $1 ]
  then
    echo "Creating configuration path $1"
    sudo mkdir -p $1
    take_ownership $1
  fi
}

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
  src=$1
  dest=$2
  admin=false
  if [ $# -eq 3 ]
  then
    admin=true
  fi


  echo "Creating a symbolic link from $src to $dest "
  if $admin
  then
      ln -hfFs $src $dest
  else
      sudo ln -hfFs $src $dest
  fi
}
