/*
 * Copyright (C) 2013 Near Infinity Corporation
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


#include "HoneycombHandler.h"
#include "JavaFrame.h"
#include "Logging.h"
#include "Java.h"
#include "Macros.h"
#include "Row.h"
#include "JNICache.h"
#include "HoneycombShare.h"
#include <jni.h>
#include "FieldEncoder.h"

/**
 * Pack the MySQL formatted row contained in buf and table into the Avro format.
 * @param buf MySQL row in buffer format
 * @param table MySQL TABLE object holding fields to be packed
 * @param row Row object to be packed
 */
int HoneycombHandler::pack_row(uchar *buf, TABLE* table, Row& row)
{
  int ret = 0;
  ret |= row.reset();
  if(table->next_number_field && buf == table->record[0])
  {
    ret |= update_auto_increment();
    if(ret)
    {
      return ret;
    }
  }

  size_t actualFieldSize;
  uchar* byte_val;

  for (Field **field_ptr = table->field; *field_ptr; field_ptr++)
  {
    Field * field = *field_ptr;
    my_ptrdiff_t offset = (my_ptrdiff_t) (buf - table->record[0]);
    field->move_field_offset(offset);

    if (field->is_null())
    {
      row.add_null();
      field->move_field_offset(-offset);
      continue;
    }

    FieldEncoder* encoder = FieldEncoder::create_encoder(*field);
    encoder->encode_field_for_writing(&byte_val, &actualFieldSize);
    row.add_value((char*)byte_val, actualFieldSize);
    delete encoder;
    MY_FREE(byte_val);
    field->move_field_offset(-offset);
  }

  return 0;
}

/**
 * Check if a row would violate a uniqueness constraint on the table if it were
 * inserted or updated.  Sets the handler's failed_key_index field if the
 * constraint is violated.
 */
bool HoneycombHandler::violates_uniqueness(jbyteArray serialized_row)
{
  JavaFrame frame(env, table->s->keys);
  for (uint i = 0; i < table->s->keys; i++) // for all indices
  {
    if (table->key_info[i].flags & HA_NOSAME) // filter on uniqueness
    {
      jstring index_name = string_to_java_string(env, table->key_info[i].name);
      bool contains_duplicate = env->CallBooleanMethod(handler_proxy,
          cache->handler_proxy().index_contains_duplicate, index_name,
          serialized_row);
        check_exceptions(env, cache, "HoneycombHandler::violates_uniqueness");
      if (contains_duplicate)
      {
        this->failed_key_index = i;
        return true;
      }
    }
  }
  return false;
}

int HoneycombHandler::write_row(uchar *buf)
{
  DBUG_ENTER("HoneycombHandler::write_row");
  JavaFrame frame(env, 1);
  row->reset();
  int rc = 0;

  if (share->crashed)
  {
    return HA_ERR_CRASHED_ON_USAGE;
  }

  my_bitmap_map *old_map = dbug_tmp_use_all_columns(table, table->read_set);
  rc |= pack_row(buf, table, *row);
  dbug_tmp_restore_column_map(table->read_set, old_map);

  jbyteArray serialized_row = serialize_to_java(env, *row);

  if (!rc && violates_uniqueness(serialized_row))
  {
    rc = HA_ERR_FOUND_DUPP_KEY;
  } else {
    env->CallVoidMethod(handler_proxy, cache->handler_proxy().insert_row,
        serialized_row);
    rc |= check_exceptions(env, cache, "HoneycombHandler::write_row");
  }
  if (rc) {
    DBUG_RETURN(rc);
  }
  else {
    ha_statistic_increment(&SSV::ha_write_count);
    DBUG_RETURN(rc);
  }
}

int HoneycombHandler::update_row(const uchar *old_row, uchar *new_row)
{
  DBUG_ENTER("HoneycombHandler::update_row");
  int rc = 0;

  Row old_sql_row;
  row->reset();
  my_bitmap_map *old_map = dbug_tmp_use_all_columns(table, table->read_set);
  rc |= pack_row(new_row, table, *row);
  dbug_tmp_restore_column_map(table->read_set, old_map);

  old_map = dbug_tmp_use_all_columns(table, table->read_set);
  rc |= pack_row(const_cast<uchar*>(old_row), table, old_sql_row);
  dbug_tmp_restore_column_map(table->read_set, old_map);
  rc |= row->set_UUID(this->ref);
  if (rc)
  {
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  JavaFrame frame(env, 2);
  jbyteArray serialized_row = serialize_to_java(env, *row);
  jbyteArray old_serialized_row = serialize_to_java(env, old_sql_row);

  if (thd_sql_command(ha_thd()) == SQLCOM_UPDATE) // Taken when actual update, not an ON DUPLICATE KEY UPDATE
  {
    if (violates_uniqueness(serialized_row))
    {
      DBUG_RETURN(HA_ERR_FOUND_DUPP_KEY);
    }
  }

  env->CallVoidMethod(handler_proxy, cache->handler_proxy().update_row,
       old_serialized_row, serialized_row);
  rc |= check_exceptions(env, cache, "HoneycombHandler::update_row");
  if (rc) {
    DBUG_RETURN(rc);
  }
  else {
    ha_statistic_increment(&SSV::ha_update_count);
    DBUG_RETURN(rc);
  }
}

/**
 * Called by MySQL when the last scanned row should be deleted.
 */
int HoneycombHandler::delete_row(const uchar *buf)
{
  DBUG_ENTER("HoneycombHandler::delete_row");
  ha_statistic_increment(&SSV::ha_delete_count);

  Row row;
  JavaFrame frame(env, 1);
  my_bitmap_map *old_map = dbug_tmp_use_all_columns(table, table->read_set);
  pack_row(const_cast<uchar*>(buf), table, row);
  dbug_tmp_restore_column_map(table->read_set, old_map);
  row.set_UUID(ref);
  jbyteArray serialized_row = serialize_to_java(env, row);

  this->env->CallVoidMethod(handler_proxy, cache->handler_proxy().delete_row, serialized_row);
  DBUG_RETURN(check_exceptions(env, cache, "HoneycombHandler::delete_row"));
}

int HoneycombHandler::delete_all_rows()
{
  DBUG_ENTER("HoneycombHandler::delete_all_rows");
  env->CallVoidMethod(handler_proxy, cache->handler_proxy().delete_all_rows);
  DBUG_RETURN(check_exceptions(env, cache, "HoneycombHandler::delete_all_rows"));
}

int HoneycombHandler::truncate()
{
  DBUG_ENTER("HoneycombHandler::truncate");
  env->CallVoidMethod(handler_proxy, cache->handler_proxy().truncate_table);
  DBUG_RETURN(check_exceptions(env, cache, "HoneycombHandler::truncate"));
}
