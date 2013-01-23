#!/bin/bash
[ $# -eq 2 ] || { echo "Usage: $0 <HBaseAdapter directory> <honeycomb lib>"; exit 1; }

src=$1
honeycomb_lib=$2
[ -d $honeycomb_lib ] || { 
echo "Creating $honeycomb_lib directory."; 
current_user=`whoami`
sudo mkdir -p $honeycomb_lib; 
sudo chown -R $current_user:$current_user $honeycomb_lib; }

echo "Moving jars into $honeycomb_lib"
cp -R $src/target/lib $honeycomb_lib
cp $src/target/mysqlengine-0.1.jar $honeycomb_lib

echo "Making $honeycomb_lib jars executable"
chmod a+x $honeycomb_lib/lib/*.jar
chmod a+x $honeycomb_lib/*.jar
