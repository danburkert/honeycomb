cd $MYSQL_HOME/mysql-test/suite/cloud-test/r/
rm *

cd $MYSQL_HOME/mysql-test
./mtr --suite=cloud-test --extern socket=/tmp/mysql.sock --record tinyint smallint mediumint int bigint

cd $MYSQL_HOME/mysql-test/suite/cloud-test/t/
sed -i.cloud s/cloud/InnoDB/ enginetype.inc
rm enginetype.inc.cloud

cd $MYSQL_HOME/mysql-test/suite/cloud-test/r/
sed -i.InnoDB 's/InnoDB/cloud/g' *.result

cd $MYSQL_HOME/mysql-test/suite/cloud-test/t/
sed -i.InnoDB s/InnoDB/cloud/ enginetype.inc
