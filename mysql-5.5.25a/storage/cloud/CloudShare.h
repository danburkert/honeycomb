#ifndef CLOUD_SHARE_H
#define CLOUD_SHARE_H

typedef struct st_cloud_share {
  char *table_name;
  uint table_name_length;
  char *table_alias;
  char *path_to_table;
  char data_file_name[FN_REFLEN];
  uint table_path_length, table_alias_length, use_count;
  my_bool is_log_table;
  mysql_mutex_t mutex;
  THR_LOCK lock;
  bool crashed;             /* Meta file is crashed */
  ha_rows rows_recorded;    /* Number of rows in tables */
  double hbase_time;
  uint hbase_calls;
  ha_rows rows_to_insert;
  ha_rows rows_inserted;
} CloudShare;

#endif
