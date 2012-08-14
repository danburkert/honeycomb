#!/bin/zsh
cd build
make
cd ../HBaseAdapter/
mvn package assembly:single
