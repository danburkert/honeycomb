read -p "row count suffix: " suffix

echo \
"localhost
a
a
hbase
hc_$suffix
first_name,last_name,address,zip,state,country,phone,salary,fk
hdfs:///tmp/hfiles-$suffix
hdfs:///tmp/person-$suffix.csv
," | $HONEYCOMB_SOURCE/scripts/bulkload.sh
