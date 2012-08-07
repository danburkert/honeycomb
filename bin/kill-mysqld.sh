ps ax | grep -E "/bin/(mysqld|mysqld_safe)" | awk '{ print $1 }' | while read pid; do sudo kill -9 $pid; done
