#include "HoneycombHandler.h"
#include "FieldMetadata.h"
#include "JavaFrame.h"
#include "Java.h"
#include "Logging.h"
#include "Macros.h"
#include "JNISetup.h"

/**
 * @brief Create the Java object for the multi-column index.
 *
 * @param table_arg SQL Table
 *
 * @return Multi-column index object
 */
jobject HoneycombHandler::create_multipart_keys(TABLE* table_arg)
{
  uint keys = table_arg->s->keys;
  jobject java_keys = env->NewObject(cache->table_multipart_keys().clazz,
      cache->table_multipart_keys().init);
  NULL_CHECK_ABORT(java_keys, "HoneycombHandler::create_multipart_keys: OutOfMemoryError while calling NewObject");
  JavaFrame frame(env, keys);
  for (uint key = 0; key < keys; key++)
  {
    char* name = index_name(table_arg, key);
    jboolean is_unique = (table_arg->key_info + key)->flags & HA_NOSAME;
    jstring jname = string_to_java_string(name);
    this->env->CallVoidMethod(java_keys,
        cache->table_multipart_keys().add_multipart_key, jname, is_unique);
    EXCEPTION_CHECK("HoneycombHandler::create_multipart_keys", "calling addMultipartKey");
    ARRAY_DELETE(name);
  }
  return java_keys;
}

/**
 * Returned jobject is a local ref that must be deleted by caller.
 */
jobject HoneycombHandler::create_multipart_key(KEY* key, KEY_PART_INFO* key_part,
    KEY_PART_INFO* key_part_end, uint key_parts)
{
  jobject java_keys = env->NewObject(cache->table_multipart_keys().clazz,
      cache->table_multipart_keys().init);
  NULL_CHECK_ABORT(java_keys, "HoneycombHandler::create_multipart_key: OutOfMemoryError while calling NewObject");
  char* name = index_name(key_part, key_part_end, key_parts);
  jboolean is_unique = key->flags & HA_NOSAME;
  this->env->CallVoidMethod(java_keys, cache->table_multipart_keys().add_multipart_key,
      string_to_java_string(name), is_unique);
  EXCEPTION_CHECK("HoneycombHandler::create_multipart_key", "calling addMultipartKey");
  ARRAY_DELETE(name);
  return java_keys;
}

int HoneycombHandler::write_row(uchar *buf)
{
  DBUG_ENTER("HoneycombHandler::write_row");
  my_bitmap_map *old_map = dbug_tmp_use_all_columns(table, table->read_set);
  int rc = write_row(buf, NULL);
  dbug_tmp_restore_column_map(table->read_set, old_map);
  DBUG_RETURN(rc);
}

/**
 * Pack the MySQL formatted row contained in buf and table into the Avro format.
 * @param buf MySQL row in buffer format
 * @param table MySQL TABLE object holding fields to be packed
 * @param row Row object to be packed
 */
int HoneycombHandler::pack_row(uchar *buf, TABLE* table, Row* row)
{
  row->reset();
  if (table->timestamp_field_type & TIMESTAMP_AUTO_SET_ON_INSERT)
  {
    table->timestamp_field->set_time();
  }
  if(table->next_number_field && buf == table->record[0])

  {
    int res;
    if(res = update_auto_increment())
    {
      return res;
    }
  }

  size_t actualFieldSize;
  char* byte_val;

  for (Field **field_ptr = table->field; *field_ptr; field_ptr++)
  {
    Field * field = *field_ptr;
    const char* field_name = field->field_name;

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
    row->set_bytes_record(field_name, byte_val, actualFieldSize);
    MY_FREE(byte_val);
  }

  return 0;
}

int HoneycombHandler::write_row(uchar* buf, jobject updated_fields)
{
  if (share->crashed)
  {
    return HA_ERR_CRASHED_ON_USAGE;
  }

  ha_statistic_increment(&SSV::ha_write_count);

  int fields = count_fields(table);
  JavaFrame frame(env, 2*fields + 3);
  jclass adapter_class = cache->hbase_adapter().clazz;
  jmethodID write_row_method = cache->hbase_adapter().write_row;
  jstring table_name = this->table_name();
  jlong new_autoincrement_value = -1;

  jobject java_row_map = env->NewObject(cache->tree_map().clazz,
      cache->tree_map().init);
  jobject unique_values_map = env->NewObject(cache->tree_map().clazz,
      cache->tree_map().init);

  if (table->timestamp_field_type & TIMESTAMP_AUTO_SET_ON_INSERT)
  {
    table->timestamp_field->set_time();
  }

  if(table->next_number_field && buf == table->record[0])
  {
    int res;
    if((res = update_auto_increment()))
    {
      return res;
    }
  }

  uint actualFieldSize;

  for (Field **field_ptr = table->field; *field_ptr; field_ptr++)
  {
    Field * field = *field_ptr;
    jstring field_name = string_to_java_string(field->field_name);

    const bool is_null = field->is_null();
    uchar* byte_val;

    if (is_null)
    {
      env->CallVoidMethod(java_row_map, cache->tree_map().put, field_name, NULL);
      EXCEPTION_CHECK_IE("HoneycombHandler::write_row", "calling map.put");
      continue;
    }

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
      if (table->found_next_number_field == field)
      {
        new_autoincrement_value = (jlong) integral_value;
      }
      if (is_little_endian())
      {
        integral_value = bswap64(integral_value);
      }
      actualFieldSize = sizeof integral_value;
      byte_val = (uchar*) my_malloc(actualFieldSize, MYF(MY_WME));
      memcpy(byte_val, &integral_value, actualFieldSize);
      break;
    }
    case MYSQL_TYPE_FLOAT:
    case MYSQL_TYPE_DOUBLE:
    {
      double fp_value = field->val_real();
      long long* fp_ptr = (long long*) &fp_value;
      if (table->found_next_number_field == field)
      {
        new_autoincrement_value = (jlong) fp_value;
      }
      if (is_little_endian())
      {
        *fp_ptr = bswap64(*fp_ptr);
      }
      actualFieldSize = sizeof fp_value;
      byte_val = (uchar*) my_malloc(actualFieldSize, MYF(MY_WME));
      memcpy(byte_val, fp_ptr, actualFieldSize);
      break;
    }
    case MYSQL_TYPE_DECIMAL:
    case MYSQL_TYPE_NEWDECIMAL:
      actualFieldSize = field->key_length();
      byte_val = (uchar*) my_malloc(actualFieldSize, MYF(MY_WME));
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
      byte_val = (uchar*) my_malloc(actualFieldSize, MYF(MY_WME));
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
      byte_val = (uchar*) my_malloc(actualFieldSize, MYF(MY_WME));
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
      byte_val = (uchar*) my_malloc(actualFieldSize, MYF(MY_WME));
      memcpy(byte_val, string_value.ptr(), actualFieldSize);
      break;
    }
    case MYSQL_TYPE_NULL:
    case MYSQL_TYPE_BIT:
    case MYSQL_TYPE_SET:
    case MYSQL_TYPE_GEOMETRY:
    default:
      actualFieldSize = field->key_length();
      byte_val = (uchar*) my_malloc(actualFieldSize, MYF(MY_WME));
      memcpy(byte_val, field->ptr, actualFieldSize);
      break;
    }
    jbyteArray java_bytes = convert_value_to_java_bytes(byte_val,
        actualFieldSize, this->env);
    MY_FREE(byte_val);
    env->CallVoidMethod(java_row_map, cache->tree_map().put, field_name, java_bytes);
    EXCEPTION_CHECK_IE("HoneycombHandler::write_row", "calling map.put");

    // Remember this field for later if we find that it has a unique index,
    // need to check it
    if (this->field_has_unique_index(field))
    {
      env->CallVoidMethod(unique_values_map, cache->tree_map().put, field_name, java_bytes);
      EXCEPTION_CHECK_IE("HoneycombHandler::write_row", "calling map.put");
    }
  }
  if (updated_fields)
  {
    THD* thd = ha_thd();
    int command = thd_sql_command(thd);
    if(command == SQLCOM_UPDATE) // Taken when actual update, but not on ON DUPLICATE KEY UPDATE
    {
      if (this->row_has_duplicate_values(unique_values_map, updated_fields))
      {
        return HA_ERR_FOUND_DUPP_KEY;
      }
    }
    jmethodID update_row_method = cache->hbase_adapter().update_row;
    jbyteArray uuid = convert_value_to_java_bytes(this->ref, 16, this->env);
    this->env->CallStaticBooleanMethod(adapter_class, update_row_method,
        this->curr_write_id, uuid, updated_fields, table_name,
        java_row_map);
    EXCEPTION_CHECK_IE("HoneycombHandler::write_row", "calling updateRow");
  }
  else
  {
    if (this->row_has_duplicate_values(unique_values_map, updated_fields))
    {
      return HA_ERR_FOUND_DUPP_KEY;
    }
    this->env->CallStaticBooleanMethod(adapter_class, write_row_method,
        this->curr_write_id, table_name, java_row_map);
    EXCEPTION_CHECK_IE("HoneycombHandler::write_row", "calling writeRow");
    this->rows_written++;
  }
  if (new_autoincrement_value >= 0 && new_autoincrement_value < LLONG_MAX)
  {
    set_autoinc_counter(new_autoincrement_value + 1, JNI_FALSE);
  }
  else if (new_autoincrement_value >= 0)
  {
    set_autoinc_counter(new_autoincrement_value, JNI_FALSE);
  }

  return 0;
}

bool HoneycombHandler::row_has_duplicate_values(jobject value_map,
    jobject changedColumns)
{
  this->flush_writes(); // Flush before checking for duplicates to make sure the changes are in HBase.
  EXCEPTION_CHECK("row_has_duplicate_values", "flush_writes");
  JavaFrame frame(env, 1);
  jclass adapter_class = cache->hbase_adapter().clazz;
  jmethodID has_duplicates_method;
  jstring duplicate_column;
  if(changedColumns == NULL)
  {
    has_duplicates_method = cache->hbase_adapter().find_duplicate_key;
    duplicate_column = (jstring) this->env->CallStaticObjectMethod(adapter_class,
        has_duplicates_method, this->table_name(), value_map);
    EXCEPTION_CHECK_IE("HoneycombHandler::row_has_duplicate_values", "calling findDuplicateKey");
  }
  else
  {
    has_duplicates_method = cache->hbase_adapter().find_duplicate_key_list;
    duplicate_column = (jstring) this->env->CallStaticObjectMethod(adapter_class,
        has_duplicates_method, this->table_name(), value_map, changedColumns);
    EXCEPTION_CHECK_IE("HoneycombHandler::row_has_duplicate_values", "calling findDuplicateKey (list)");
  }

  bool error = duplicate_column != NULL;

  if (error)
  {
    const char *key_name = this->java_to_string(duplicate_column);
    this->failed_key_index = this->get_failed_key_index(key_name);
    this->env->ReleaseStringUTFChars(duplicate_column, key_name);
  }

  return error;
}

/*
 This will be called in a table scan right before the previous ::rnd_next()
 call.
 */
int HoneycombHandler::update_row(const uchar *old_row, uchar *new_row)
{
  DBUG_ENTER("HoneycombHandler::update_row");
  ha_statistic_increment(&SSV::ha_update_count);
  my_bitmap_map *old_map;
  if (table->timestamp_field_type & TIMESTAMP_AUTO_SET_ON_UPDATE)
  {
    table->timestamp_field->set_time();
  }

  JavaFrame frame(env, 1);
  jobject updated_fieldnames = env->NewObject(cache->linked_list().clazz,
      cache->linked_list().init);
  this->collect_changed_fields(updated_fieldnames, old_row, new_row);
  jlong size = env->CallLongMethod(updated_fieldnames, cache->linked_list().size);

  if(size == 0)
  {
    // No fields have actually changed. Don't write a new row.
    DBUG_RETURN(0);
  }

  old_map = dbug_tmp_use_all_columns(table, table->read_set);
  int rc = write_row(new_row, updated_fieldnames);
  dbug_tmp_restore_column_map(table->read_set, old_map);
  this->flush_writes();
  EXCEPTION_CHECK("update_row", "flush_writes");
  DBUG_RETURN(rc);
}

/**
 * @brief Determines what fields have changed in a MySQL row on update.
 *
 * @param updated_fields Collects changed fields
 * @param old_row Old MySQL row
 * @param new_row New MySQL row
 */
void HoneycombHandler::collect_changed_fields(jobject updated_fields,
    const uchar* old_row, uchar* new_row)
{
  typedef unsigned long int ulint;
  uint n_fields = table->s->fields;
  const ulint null_field = 0xFFFFFFFF;
  JavaFrame frame(env, n_fields);
  for (uint i = 0; i < n_fields; i++)
  {
    Field* field = table->field[i];

    const uchar* old_field = (const uchar*) old_row + field->offset(table->record[0]);
    const uchar* new_field = (const uchar*) new_row + field->offset(table->record[0]);

    int col_pack_len = field->pack_length();

    ulint old_field_length = col_pack_len;
    ulint new_field_length = col_pack_len;

    if (field->null_ptr)
    {
      if (field->is_null_in_record(old_row))
      {
        old_field_length = null_field;
      }

      if (field->is_null_in_record(new_row))
      {
        new_field_length = null_field;
      }
    }

    // If field lengths are different
    // OR if original field is not NULL AND new and original fields are different
    bool field_lengths_are_different = old_field_length != new_field_length;
    bool original_not_null_and_field_has_changed = old_field_length != null_field
      && 0 != memcmp(old_field, new_field, old_field_length);
    if (field_lengths_are_different || original_not_null_and_field_has_changed)
    {
      env->CallBooleanMethod(updated_fields, cache->linked_list().add,
          string_to_java_string(field->field_name));
    }
  }
}

int HoneycombHandler::add_index(TABLE *table_arg, KEY *key_info, uint num_of_keys,
    handler_add_index **add)
{
  JavaFrame frame(env, 3*num_of_keys);
  for(uint key = 0; key < num_of_keys; key++)
  {
    KEY* pos = key_info + key;
    KEY_PART_INFO *key_part = pos->key_part;
    KEY_PART_INFO *end_key_part = key_part + key_info->key_parts;
    char* index_columns = this->index_name(key_part, end_key_part,
        key_info->key_parts);

    Field *field_being_indexed = key_info->key_part->field;
    if (pos->flags & HA_NOSAME)
    {
      jbyteArray duplicate_value = this->find_duplicate_column_values(index_columns);

      int error = duplicate_value != NULL ? HA_ERR_FOUND_DUPP_KEY : 0;

      if (error == HA_ERR_FOUND_DUPP_KEY)
      {
        int length = (int)this->env->GetArrayLength(duplicate_value);
        char *value_key = char_array_from_java_bytes(duplicate_value, this->env);
        this->store_field_value(field_being_indexed, value_key, length);
        ARRAY_DELETE(value_key);
        ARRAY_DELETE(index_columns);
        this->failed_key_index = this->get_failed_key_index(key_part->field->field_name);

        return error;
      }
    }

    jclass adapter = cache->hbase_adapter().clazz;
    jobject java_keys = this->create_multipart_key(pos, key_part, end_key_part,
        key_info->key_parts);
    jmethodID add_index_method = cache->hbase_adapter().add_index;
    jstring table_name = this->table_name();
    this->env->CallStaticVoidMethod(adapter, add_index_method, table_name, java_keys);
    EXCEPTION_CHECK_IE("HoneycombHandler::add_index", "calling addIndex");
    ARRAY_DELETE(index_columns);
  }

  return 0;
}

/**
 * Returned jbyteArray is a local reference which must be deleted by the caller.
 */
jbyteArray HoneycombHandler::find_duplicate_column_values(char* columns)
{
  int err = env->EnsureLocalCapacity(2);
  CHECK_JNI_ABORT(err, "HoneycombHandler::find_duplicate_column_values: OutOfMemoryError error while calling EnsureLocalCapacity");

  jclass adapter = cache->hbase_adapter().clazz;
  jmethodID column_has_duplicates_method = cache->hbase_adapter().find_duplicate_value;
  jstring jcolumns = string_to_java_string(columns);
  jstring jtable_name = this->table_name();

  jbyteArray duplicate_value = (jbyteArray) this->env->CallStaticObjectMethod(adapter,
      column_has_duplicates_method, jtable_name, jcolumns);
  EXCEPTION_CHECK("HoneycombHandler::find_duplicate_value", "calling findDuplicateValue");

  env->DeleteLocalRef(jcolumns);
  env->DeleteLocalRef(jtable_name);
  return duplicate_value;
}


int HoneycombHandler::prepare_drop_index(TABLE *table_arg, uint *key_num, uint num_of_keys)
{
  JavaFrame frame(env, num_of_keys + 1);
  jclass adapter = cache->hbase_adapter().clazz;
  jmethodID drop_index_method = cache->hbase_adapter().drop_index;
  jstring table_name = this->table_name();
  for (uint key = 0; key < num_of_keys; key++)
  {
    char* name = index_name(table_arg, key_num[key]);
    jstring jname = string_to_java_string(name);
    this->env->CallStaticVoidMethod(adapter, drop_index_method, table_name, jname);
    EXCEPTION_CHECK_IE("HoneycombHandler::prepare_drop_index", "calling dropIndex");
    ARRAY_DELETE(name);
  }
  return 0;
}


/**
 * Called by MySQL when the last scanned row should be deleted.
 */
int HoneycombHandler::delete_row(const uchar *buf)
{
  DBUG_ENTER("HoneycombHandler::delete_row");
  ha_statistic_increment(&SSV::ha_delete_count);

  JavaFrame frame(env, 2);
  jstring table_name = this->table_name();
  jbyteArray pos = convert_value_to_java_bytes(this->ref, 16, this->env);

  jclass adapter_class = cache->hbase_adapter().clazz;
  jmethodID delete_row_method = cache->hbase_adapter().delete_row;
  this->env->CallStaticBooleanMethod(adapter_class, delete_row_method, table_name, pos);
  EXCEPTION_CHECK_IE("HoneycombHandler::delete_row", "calling deleteRow");

  DBUG_RETURN(0);
}

int HoneycombHandler::delete_all_rows()
{
  DBUG_ENTER("HoneycombHandler::delete_all_rows");

  JavaFrame frame(env);
  jstring table_name = this->table_name();
  jclass adapter_class = cache->hbase_adapter().clazz;
  jmethodID delete_all_rows = cache->hbase_adapter().delete_all_rows;
  jmethodID set_row_count = cache->hbase_adapter().set_row_count;

  this->env->CallStaticIntMethod(adapter_class, delete_all_rows, table_name);
  EXCEPTION_CHECK_IE("HoneycombHandler::delete_all_rows", "calling deleteAllRows");
  this->env->CallStaticVoidMethod(adapter_class, set_row_count, table_name, 0);
  EXCEPTION_CHECK_IE("HoneycombHandler::delete_all_rows", "calling setRowCount");
  this->flush_writes();
  EXCEPTION_CHECK("delete_all_rows", "flush_writes");

  DBUG_RETURN(0);
}

int HoneycombHandler::truncate()
{
  DBUG_ENTER("HoneycombHandler::truncate");

  set_autoinc_counter(1, JNI_TRUE);
  int returnValue = delete_all_rows();

  DBUG_RETURN(returnValue);
}

void HoneycombHandler::set_autoinc_counter(jlong new_value, jboolean is_truncate)
{
  if(table->found_next_number_field == NULL)
  {
    return;
  }

  JavaFrame frame(env, 2);
  jclass adapter_class = cache->hbase_adapter().clazz;
  jmethodID alter_autoincrement_value = cache->hbase_adapter().alter_autoincrement_value;
  jstring field_name = string_to_java_string(table->found_next_number_field->field_name);
  jstring table_name =  this->table_name();
  if (this->env->CallStaticBooleanMethod(adapter_class,
        alter_autoincrement_value, table_name, field_name,
        new_value, is_truncate))
  {
    stats.auto_increment_value = (ulonglong) new_value;
  }
  EXCEPTION_CHECK("HoneycombHandler::set_autoinc_counter", "calling alterAutoincrementValue");
}

void HoneycombHandler::update_create_info(HA_CREATE_INFO* create_info)
{
  DBUG_ENTER("HoneycombHandler::update_create_info");

  //show create table
  if (!(create_info->used_fields & HA_CREATE_USED_AUTO)) {
    HoneycombHandler::info(HA_STATUS_AUTO);
    create_info->auto_increment_value = stats.auto_increment_value;
  }
  //alter table
  else if (create_info->used_fields == 1) {
    set_autoinc_counter(create_info->auto_increment_value, JNI_FALSE);
  }

  DBUG_VOID_RETURN;
}
