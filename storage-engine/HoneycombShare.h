/*
 * Copyright (C) 2013 Altamira Corporation
 *
 * This file is part of Honeycomb Storage Engine.
 *
 * Honeycomb Storage Engine is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * Honeycomb Storage Engine is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Honeycomb Storage Engine.  If not, see <http://www.gnu.org/licenses/>.
 */


#ifndef HONEYCOMB_SHARE_H
#define HONEYCOMB_SHARE_H

#include "my_global.h"
#include "thr_lock.h"           /* THR_LOCK, THR_LOCK_DATA */
#include "my_base.h"
typedef struct st_honeycomb_share {
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
} HoneycombShare;

#endif
