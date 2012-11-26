# Clear out old results
cd $MYSQL_HOME/mysql-test/suite/cloud-test/r/
rm *

# Change engine to InnoDB to record baseline results
cd $MYSQL_HOME/mysql-test/suite/cloud-test/t/
sed -i.cloud s/cloud/InnoDB/ enginetype.inc
rm enginetype.inc.cloud

# Run tests and record results
cd $MYSQL_HOME/mysql-test
./mtr --suite=cloud-test --extern socket=/tmp/mysql.sock --record \
tinyint   \
smallint  \
mediumint \
int       \
bigint    \
decimal   \
float     \
double    \
char      \
varchar   \
binary    \
varbinary \
enum      \
date      \
datetime  \
timestamp \
time      \
year      \
joins     \
group_by  \
case

# Change engine to cloud in results (appears in table creation statements)
cd $MYSQL_HOME/mysql-test/suite/cloud-test/r/
sed -i.InnoDB 's/InnoDB/cloud/g' *.result
rm *.result.InnoDB

# Change engine to cloud for subsequent test runs
cd $MYSQL_HOME/mysql-test/suite/cloud-test/t/
sed -i.InnoDB s/InnoDB/cloud/ enginetype.inc
rm enginetype.inc.InnoDB

# Move test results that are manually built
cd $MYSQL_HOME/mysql-test/suite/cloud-test/
cp t/manual_results/* r/
