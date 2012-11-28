#!/bin/sh

bundle=`pwd`/honeycomb.tar.gz
honeycomb_lib=/usr/local/lib/honeycomb
askpass=sshaskpass.sh
command -v $askpass >/dev/null 2>&1 || { echo >&2 "$askpass is required to run $0."; exit 1; }

[ -d $honeycomb_lib ] || { echo "$honeycomb_lib must exist to use this script."; exit 1; }

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
