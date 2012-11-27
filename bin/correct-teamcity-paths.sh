#!/bin/sh

if [ -e pom.apache.xml ]; then
  mv pom.apache.xml pom.xml
fi

if [ -d cloud ]; then
  mv cloud mysql-5.5.28/storage/
fi
