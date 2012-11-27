#!/bin/sh

if [ -e pom.apache.xml ]; then
  mv -f pom.apache.xml pom.xml
fi

if [ -d cloud ]; then
  mv -f cloud mysql-5.5.28/storage/
fi
