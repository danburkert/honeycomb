#!/bin/sh

: ${HONEYCOMB_HOME?"Need to set HONEYCOMB_HOME environmental variable to the top of the project."}
command -v mvn >/dev/null 2>&1 || { echo >&2 "mvn is required to run $0."; exit 1; }

if [ $# -eq 1 ]
then
  honeycomb_lib=$1
elif [ ! -z "$HONEYCOMB_LIB" ]
then
  honeycomb_lib=$HONEYCOMB_LIB
else
  honeycomb_lib=/usr/local/lib/honeycomb
fi

cd $HONEYCOMB_HOME/HBaseAdapter
echo "Running: mvn package install"
mvn package install -Dapache
$HONEYCOMB_HOME/bin/install-honeycomb-jars.sh "`pwd`" $honeycomb_lib

echo "Setting up the classpath.conf file with the complete classpath."
create_classpath=$($HONEYCOMB_HOME/bin/create-classpath.rb $honeycomb_lib $honeycomb_lib/lib)
echo $create_classpath | sudo tee /etc/mysql/classpath.conf > /dev/null

adapter_conf=/etc/mysql/adapter.conf
if [ ! -e $adapter_conf ]
then
  echo "Creating the adapter.conf from the repository."
  sudo cp $HONEYCOMB_HOME/HBaseAdapter/adapter.conf $adapter_conf

  _zk_quorum=localhost
  read -p "Setup Zookeeper Quorum [$_zk_quorum]:" zk_quorum
  zk_quorum=${zk_quorum:-$_zk_quorum}
  sudo perl -pi -w -e "s/^zk_quorum (\w)$/zk_quorum $zk_quorum/g;" $adapter_conf
fi
echo "*** Don't forget to restart MySQL. The JVM doesn't autoreload the jar from the disk. ***"
