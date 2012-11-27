#!/bin/sh

if [ -e pom.apache.xml ]; then
  mv -f pom.apache.xml pom.xml
fi

dest=mysql-5.5.28/storage/cloud
if [ -d cloud ]; then
  if [ -h $dest ]; then
    rm $dest
  fi

  mv -f cloud $dest
fi
