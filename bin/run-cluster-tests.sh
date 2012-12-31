cd $MYSQL_HOME/mysql-test
rm suite/honeycomb-test/r/*.reject
./mtr --suite=honeycomb-test --extern host=nic-hadoop-admin --force --retry=2 --max-test-fail=100
