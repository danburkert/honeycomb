mysql=$(ps ax | grep "[m]ysqld " | awk '{ print $1 }')
echo "Attaching to pid $mysql"
sudo gdb --pid $mysql
