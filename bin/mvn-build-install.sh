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

if [ ! -d $honeycomb_lib ]
then
  echo "Creating $honeycomb_lib directory"
  mkdir -p $honeycomb_lib
fi

cd $HONEYCOMB_HOME/HBaseAdapter
echo "Running: mvn package install"
mvn package install
echo "Moving jars into $honeycomb_lib"
cp target/lib/*.jar $honeycomb_lib
cp target/mysqlengine-0.1.jar $honeycomb_lib

echo "Making $honeycomb_lib jars executable"
chmod a+x $honeycomb_lib/*.jar

echo "Setting up the classpath.conf file with the complete classpath."
create_classpath=$($HONEYCOMB_HOME/bin/create-classpath.rb $honeycomb_lib)
echo $create_classpath | sudo tee /etc/mysql/classpath.conf > /dev/null
