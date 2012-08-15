#!/bin/zsh
make -C ~/NIC/mysql-cloud-engine/build
mvn -q package assembly:single -f ~/NIC/mysql-cloud-engine/HBaseAdapter/pom.xml
