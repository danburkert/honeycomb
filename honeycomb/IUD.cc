#include "HoneycombHandler.h"
#include "JavaFrame.h"
#include "Logging.h"
#include "Java.h"
#include "Macros.h"

/**
 * Pack the MySQL formatted row contained in buf and table into the Avro format.
 * @param buf MySQL row in buffer format
 * @param table MySQL TABLE object holding fields to be packed
 * @param row Row object to be packed
 */
int HoneycombHandler::pack_row(uchar *buf, TABLE* table, Row* row)
{
  int ret = 0;
  ret |= row->reset();
  if(table->next_number_field && buf == table->record[0])
  {
    ret |= update_auto_increment();
    if(ret)
    {
      return ret;
    }
  }

  size_t actualFieldSize;
  char* byte_val;

  for (Field **field_ptr = table->field; *field_ptr; field_ptr++)
  {
    Field * field = *field_ptr;

    if (field->is_null()) { continue; }

    switch (field->real_type())
    {
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_LONGLONG:
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_YEAR:
    case MYSQL_TYPE_ENUM:
    case MYSQL_TYPE_TIME: // Time is a special case for sorting
    {
      long long integral_value = field->val_int();
      if (is_little_endian())
      {
        integral_value = bswap64(integral_value);
      }
      actualFieldSize = sizeof integral_value;
      byte_val = (char*) my_malloc(actualFieldSize, MYF(MY_WME));
      memcpy(byte_val, &integral_value, actualFieldSize);
      break;
    }
    case MYSQL_TYPE_FLOAT:
    case MYSQL_TYPE_DOUBLE:
    {
      double fp_value = field->val_real();
      long long* fp_ptr = (long long*) &fp_value;
      if (is_little_endian())
      {
        *fp_ptr = bswap64(*fp_ptr);
      }
      actualFieldSize = sizeof fp_value;
      byte_val = (char*) my_malloc(actualFieldSize, MYF(MY_WME));
      memcpy(byte_val, fp_ptr, actualFieldSize);
      break;
    }
    case MYSQL_TYPE_DECIMAL:
    case MYSQL_TYPE_NEWDECIMAL:
      actualFieldSize = field->key_length();
      byte_val = (char*) my_malloc(actualFieldSize, MYF(MY_WME));
      memcpy(byte_val, field->ptr, actualFieldSize);
      break;
    case MYSQL_TYPE_DATE:
    case MYSQL_TYPE_NEWDATE:
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_TIMESTAMP:
    {
      MYSQL_TIME mysql_time;
      char temporal_value[MAX_DATE_STRING_REP_LENGTH];
      field->get_time(&mysql_time);
      my_TIME_to_str(&mysql_time, temporal_value);
      actualFieldSize = strlen(temporal_value);
      byte_val = (char*) my_malloc(actualFieldSize, MYF(MY_WME));
      memcpy(byte_val, temporal_value, actualFieldSize);
      break;
    }
    case MYSQL_TYPE_BLOB:
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
    {
      String string_value;
      field->val_str(&string_value);
      actualFieldSize = string_value.length();
      byte_val = (char*) my_malloc(actualFieldSize, MYF(MY_WME));
      memcpy(byte_val, string_value.ptr(), actualFieldSize);
      break;
    }
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_VAR_STRING:
    {
      char string_value_buff[field->field_length];
      String string_value(string_value_buff, sizeof(string_value_buff),&my_charset_bin);
      field->val_str(&string_value);
      actualFieldSize = string_value.length();
      byte_val = (char*) my_malloc(actualFieldSize, MYF(MY_WME));
      memcpy(byte_val, string_value.ptr(), actualFieldSize);
      break;
    }
    case MYSQL_TYPE_NULL:
    case MYSQL_TYPE_BIT:
    case MYSQL_TYPE_SET:
    case MYSQL_TYPE_GEOMETRY:
    default:
      actualFieldSize = field->key_length();
      byte_val = (char*) my_malloc(actualFieldSize, MYF(MY_WME));
      memcpy(byte_val, field->ptr, actualFieldSize);
      break;
    }
    row->set_record(field->field_name, byte_val, actualFieldSize);
    MY_FREE(byte_val);
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
  if (table->timestamp_field_type & TIMESTAMP_AUTO_SET_ON_INSERT)
  {
    table->timestamp_field->set_time();
  }

  my_bitmap_map *old_map = dbug_tmp_use_all_columns(table, table->read_set);
  rc |= pack_row(buf, table, row);
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
    this->rows_written++;
    DBUG_RETURN(rc);
  }
}

int HoneycombHandler::update_row(const uchar *old_row, uchar *new_row)
{
  DBUG_ENTER("HoneycombHandler::update_row");
  int rc = 0;
  if (table->timestamp_field_type & TIMESTAMP_AUTO_SET_ON_UPDATE)
  {
    table->timestamp_field->set_time();
  }

  row->reset();
  my_bitmap_map *old_map = dbug_tmp_use_all_columns(table, table->read_set);
  rc |= pack_row(new_row, table, row);
  dbug_tmp_restore_column_map(table->read_set, old_map);
  rc |= row->set_UUID(this->ref);
  if (rc)
  {
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  JavaFrame frame(env, 1);
  jbyteArray serialized_row = serialize_to_java(env, *row);

  if (thd_sql_command(ha_thd()) == SQLCOM_UPDATE) // Taken when actual update, not an ON DUPLICATE KEY UPDATE
  {
    if (violates_uniqueness(serialized_row))
    {
      DBUG_RETURN(HA_ERR_FOUND_DUPP_KEY);
    }
  }

  env->CallVoidMethod(handler_proxy, cache->handler_proxy().update_row,
      serialized_row);
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

  JavaFrame frame(env, 1);
  jbyteArray pos = convert_value_to_java_bytes(ref, 16, env);

  this->env->CallVoidMethod(handler_proxy, cache->handler_proxy().delete_row, pos);
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
