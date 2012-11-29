#!/bin/sh

bundle=`pwd`/honeycomb.tar.gz
honeycomb_lib=/usr/local/lib/honeycomb

[ -d $honeycomb_lib ] || { echo "$honeycomb_lib must exist to use this script."; exit 1; }

pushd $honeycomb_lib
echo "Creating the tar for honeycomb in $bundle"
tar -czf $bundle --exclude='cloud-test' .
popd

puppet=localadmin@nic-hadoop-puppet
echo "Sending the tar off to puppet"
scp $bundle $puppet:/opt/packages
echo "Extracting the tar on puppet"
ssh $puppet "cd /opt/packages; tar xzf honeycomb.tar.gz"
