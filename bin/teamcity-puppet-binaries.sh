#!/bin/sh

bundle=`pwd`/honeycomb.tar.gz
honeycomb_lib=/usr/local/lib/honeycomb
askpass=./sshaskpass.sh

[ -d $honeycomb_lib ] || { echo "$honeycomb_lib must exist to use this script."; exit 1; }
[ -e $askpass ] || { echo "$askpass is required for this script to run. Current directory `pwd` and contents `ls -m`"; exit 1; }

pushd $honeycomb_lib
echo "Creating the tar for honeycomb"
tar -czf $bundle --exclude='cloud-test' .
popd

password="password"
puppet=localadmin@nic-hadoop-puppet
echo "Sending the tar off to puppet"
echo $password | $askpass scp $bundle $puppet:/opt/packages
echo "Extracting the tar on puppet"
echo $password | $askpass ssh $puppet "cd /opt/packages; tar xzf $bundle"
