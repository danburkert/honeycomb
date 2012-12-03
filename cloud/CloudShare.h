#ifndef CLOUD_SHARE_H
#define CLOUD_SHARE_H

#include "my_global.h"
#include "thr_lock.h"           /* THR_LOCK, THR_LOCK_DATA */
#include "my_base.h"
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
} CloudShare;

#endif
