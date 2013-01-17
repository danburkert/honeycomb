
zk_quorum=localhost
honeycomb_hb_table=sql
honeycomb_sql_columns=first_name,last_name,address,zip,state,country,phone,salary,fk

read -p "MySQL table name: " honeycomb_sql_table

_importtsv_bulk_output=hdfs:///tmp/hfiles
read -p "HFile output path [$_importtsv_bulk_output]:" importtsv_bulk_output
importtsv_bulk_output=${importtsv_bulk_output:-$_importtsv_bulk_output}

_input_folder=hdfs:///tmp/person-05.csv
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
  '-D importtsv.separator=,' \
  $honeycomb_hb_table \
  $input_folder
