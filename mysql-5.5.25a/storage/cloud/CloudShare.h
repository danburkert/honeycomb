#ifndef CLOUD_SHARE_H
#define CLOUD_SHARE_H

typedef struct st_cloud_share {
  char *table_name;
  uint table_name_length, use_count;
  my_bool is_log_table;
  mysql_mutex_t mutex;
  THR_LOCK lock;
  bool crashed;             /* Meta file is crashed */
  ha_rows rows_recorded;    /* Number of rows in tables */
} CloudShare;

#endif
