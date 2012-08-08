mysql=$(ps ax | grep "/bin/mysqld " | head -n 1 | awk '{ print $1 }')
sudo gdb --pid $mysql
