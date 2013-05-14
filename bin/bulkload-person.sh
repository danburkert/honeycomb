honeycomb_hb_table=a
honeycomb_hb_cf=a
honeycomb_sql_columns=first_name,last_name,address,zip,state,country,phone,salary,fk

read -p "MySQL table name: " honeycomb_sql_table

_importtsv_bulk_output=hdfs:///tmp/hfiles-05
read -p "HFile output path [$_importtsv_bulk_output]:" importtsv_bulk_output
importtsv_bulk_output=${importtsv_bulk_output:-$_importtsv_bulk_output}

_input_folder=hdfs:///tmp/person-05.csv
read -p "Input folder path [$_input_folder]:" input_folder
input_folder=${input_folder:-$_input_folder}

hbase org.apache.hadoop.hbase.mapreduce.ImportTsv \
  -D importtsv.columns=a,HBASE_ROW_KEY \
  -D importtsv.mapper.class=com.nearinfinity.honeycomb.hbase.bulkload.BulkLoadMapper \
  -D honeycomb.sql.table=$honeycomb_sql_table \
  -D honeycomb.sql.columns=$honeycomb_sql_columns \
  -D honeycomb.hbase.tableName=$honeycomb_hb_table \
  -D honeycomb.hbase.columnFamily=$honeycomb_hb_cf \
  -D importtsv.bulk.output=$importtsv_bulk_output \
  '-D importtsv.separator=,' \
  $honeycomb_hb_table \
  $input_folder
