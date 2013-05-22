#!/bin/bash

set -e
: ${JAVA_HOME?"$JAVA_HOME must be set so Honeycomb can run."}

function find_java_dir
{
    local dir=$(find $JAVA_HOME/ -name $1 -type d | head -1)
    if [ -z "$dir" ]
    then
        echo "Could not find $1 under $JAVA_HOME. Make sure $JAVA_HOME has a jre under it."
        exit 1
    fi
    eval "$2=$dir"
}

function find_file
{
    file=$(find $1 -name $2 -type f | head -1)
    if [ -z "$file" ]
    then
        echo "$2 could not be found under $1."
        exit 1
    fi

    echo "$file"
}

base_dir="$(cd $(dirname "${BASH_SOURCE[0]}") && pwd)"

install_db=$(find_file $base_dir "mysql_install_db")
mysql_client=$(find_file $base_dir "mysql")
install_db_run=$(find $base_dir -path "*data/mysql" -type d | head -1)
if [ -z "$install_db_run" ]
then
    $install_db --user=$(whoami)
fi

mysql_start=$(find_file $base_dir "mysqld_safe")
xawt=""
server=""
amd64=""
find_java_dir "xawt" xawt
find_java_dir "server" server
find_java_dir "amd64" amd64

LD_LIBRARY_PATH=$amd64:$xawt:$server $mysql_start &
echo "MySQL is up and running"

counter=0
socket=/tmp/mysql.sock
echo -n "Looking for $socket "
while [ ! -e $socket ]
do
    if [ $counter -gt 8 ]
    then
        echo "MySQL did not come up in a reasonable amount of time."
        exit 1
    fi
    sleep 1
    counter=$(($counter + 1))
    echo -n "."
done

test_file=$base_dir/honeycomb-installed
if [ ! -e $test_file ]
then
    honeycomb_install=/tmp/honeycomb-install
    echo  "install plugin honeycomb soname 'ha_honeycomb.so'" > $honeycomb_install
    $mysql_client -u root < $honeycomb_install
    rm $honeycomb_install
    touch $test_file
    echo "Honeycomb is now installed"
fi
