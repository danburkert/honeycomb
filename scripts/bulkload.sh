IFS=''

_zk_quorum=localhost
read -p "Zookeeper Quorum [$_zk_quorum]:" zk_quorum
zk_quorum=${zk_quorum:-$_zk_quorum}

read -p "HBase table: " honeycomb_hb_table
read -p "HBase column family: " honeycomb_hb_cf
read -p "MySQL database: " honeycomb_sql_db
read -p "MySQL table: " honeycomb_sql_table
read -p "MySQL table columns (comma seperated): " honeycomb_sql_columns

_importtsv_bulk_output=hdfs:///tmp/hfiles
read -p "HFile output path [$_importtsv_bulk_output]:" importtsv_bulk_output
importtsv_bulk_output=${importtsv_bulk_output:-$_importtsv_bulk_output}

_input_folder=hdfs:///tmp/input/
read -p "Input folder path [$_input_folder]:" input_folder
input_folder=${input_folder:-$_input_folder}

read -p "Input separator: " separator

hbase org.apache.hadoop.hbase.mapreduce.ImportTsv \
  -D importtsv.columns=HBASE_ROW_KEY,unused \
  -D importtsv.mapper.class=com.nearinfinity.honeycomb.hbase.bulkload.BulkLoadMapper \
  -D hbase.zookeeper.quorum=$zk_quorum \
  -D honeycomb.hbase.tableName=$honeycomb_hb_table \
  -D honeycomb.hbase.columnFamily=$honeycomb_hb_cf \
  -D honeycomb.sql.table=$honeycomb_sql_db/$honeycomb_sql_table \
  -D honeycomb.sql.columns=$honeycomb_sql_columns \
  -D importtsv.bulk.output=$importtsv_bulk_output \
  "-D importtsv.separator=$separator" \
  $honeycomb_hb_table \
  $input_folder
