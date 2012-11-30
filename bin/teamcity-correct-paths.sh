#!/bin/sh

if [ -h pom.xml ]; then
  rm pom.xml
  ln -s pom.cdh.xml pom.xml
fi
