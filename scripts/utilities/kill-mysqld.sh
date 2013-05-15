ps ax | grep -E "/bin/(mysqld|mysqld_safe)" | awk '{ print $1 }' | while read pid; do sudo kill -9 $pid; done
if [ -e /tmp/mysql.sock ]; then
  sudo rm /tmp/mysql.sock
fi

process=`ps ax | grep -E "/bin/(mysqld|mysqld_safe)"`

if [ ! -z $process ]; then
  sudo mysql.server stop
fi
