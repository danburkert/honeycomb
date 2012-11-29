#!/bin/sh

if [ -e pom.apache.xml ]; then
  mv -f pom.cdh.xml pom.xml
fi

dest=mysql-5.5.28/storage/cloud
if [ -d cloud ]; then
  if [ -e $dest ]; then
    echo "Removing $dest"
    rm $dest
  fi

  mv -f cloud $dest
fi
