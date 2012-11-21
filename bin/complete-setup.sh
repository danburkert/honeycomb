#!/bin/sh

function check_env
{
  : ${$1?"Need to set $1 environmental variable"}
}

_honeycomb_lib=/usr/local/lib/honeycomb
read -p "Honeycomb jar path [$_honeycomb_lib]:" honeycomb_lib
honeycomb_lib=${honeycomb_lib:-$_honeycomb_lib}

if [ ! -d $honeycomb_lib ]
then
  echo "Creating $honeycomb_lib directory"
  mkdir -p $honeycomb_lib
fi

check_env HONEYCOMB_HOME
