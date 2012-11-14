_zk_quorum=localhost
read -p "Zookeeper Quorum [$_zk_quorum]:" zk_quorum
zk_quorum=${zk_quorum:-$_zk_quorum}

_honeycomb_hb_table=sql
read -p "HBase table name [$_honeycomb_hb_table]:" honeycomb_hb_table
honeycomb_hb_table=${honeycomb_hb_table:-$_honeycomb_hb_table}


read -p "MySQL table name: " honeycomb_sql_table
read -p "MySQL table columns (comma seperated): " honeycomb_sql_columns

_importtsv_bulk_output=hdfs:///tmp/hfiles
read -p "HFile output path [$_importtsv_bulk_output]:" importtsv_bulk_output
importtsv_bulk_output=${importtsv_bulk_output:-$_importtsv_bulk_output}

_input_folder=hdfs:///tmp/input/
read -p "Input folder path [$_input_folder]:" input_folder
input_folder=${input_folder:-$_input_folder}

hbase org.apache.hadoop.hbase.mapreduce.ImportTsv \
-D importtsv.columns=a,HBASE_ROW_KEY \
-D importtsv.mapper.class=com.nearinfinity.honeycomb.BulkLoadMapper \
-D zk.quorum=$zk_quorum \
-D honeycomb.sql.table=$honeycomb_sql_table \
-D honeycomb.sql.columns=$honeycomb_sql_columns \
-D honeycomb.hb.table=$honeycomb_hb_table \
-D importtsv.bulk.output=$importtsv_bulk_output \
unused \
$input_folder
