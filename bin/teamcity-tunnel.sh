#!/bin/sh

[ $# -eq 1 ] || { echo "Usage: $0 <nic-hadoop-admin user name>"; exit 1; }

user=$1
ssh -f -N -q -L 8111:nic-hadoop-r310-01:8111 $user@nic-hadoop-admin

echo "Go to localhost:8111 to access TeamCity"
