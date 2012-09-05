ps ax | grep -E "/bin/(mysqld|mysqld_safe)" | awk '{ print $1 }' | while read pid; do sudo kill -9 $pid; done
if [ -e /tmp/mysql.sock ]; then
  sudo rm /tmp/mysql.sock
fi

sudo mysql.server stop
