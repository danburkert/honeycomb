#include "CloudHandler.h"

int CloudHandler::index_init(uint idx, bool sorted)
{
  DBUG_ENTER("CloudHandler::index_init");

  this->active_index = idx;

  KEY *pos = table->s->key_info + idx;
  KEY_PART_INFO *key_part = pos->key_part;
  KEY_PART_INFO *key_part_end = key_part + pos->key_parts;
  const char* column_names = this->index_name(key_part, key_part_end, pos->key_parts);
  Field *field = table->field[idx];
  attach_thread();

  jclass adapter_class = this->adapter();
  jmethodID start_scan_method = find_static_method(adapter_class, "startIndexScan", "(Ljava/lang/String;Ljava/lang/String;)J",this->env);
  jstring table_name = this->table_name();
  jstring java_column_names = this->string_to_java_string(column_names);

  this->curr_scan_id = this->env->CallStaticLongMethod(adapter_class, start_scan_method, table_name, java_column_names);
  ARRAY_DELETE(column_names);

  DBUG_RETURN(0);
}

int CloudHandler::index_end()
{
  DBUG_ENTER("CloudHandler::index_end");

  this->end_scan();
  this->detach_thread();
  this->reset_index_scan_counter();

  DBUG_RETURN(0);
}

jobject CloudHandler::create_key_value_list(int index, uint* key_sizes, uchar** key_copies, const char** key_names, jboolean* key_null_bits, jboolean* key_is_null)
{
  jobject key_values = create_java_list(this->env);
  jclass key_value_class = this->env->FindClass(HBASECLIENT "KeyValue");
  jmethodID key_value_ctor = this->env->GetMethodID(key_value_class, "<init>", "(Ljava/lang/String;[BZZ)V");
  for(int x = 0; x < index; x++)
  {
    jbyteArray java_key = this->env->NewByteArray(key_sizes[x]);
    this->env->SetByteArrayRegion(java_key, 0, key_sizes[x], (jbyte*) key_copies[x]);
    jstring key_name = string_to_java_string(key_names[x]);
    jobject key_value = this->env->NewObject(key_value_class, key_value_ctor, key_name, java_key, key_null_bits[x], key_is_null[x]);
    java_list_insert(key_values, key_value, this->env);
  }

  return key_values;
}

int CloudHandler::index_read_map(uchar * buf, const uchar * key,
    key_part_map keypart_map, enum ha_rkey_function find_flag)
{
  DBUG_ENTER("CloudHandler::index_read_map");
  jclass adapter_class = this->adapter();
  jmethodID index_read_method = find_static_method(adapter_class,
      "indexRead", "(JLjava/util/List;L" MYSQLENGINE "IndexReadType;)L" MYSQLENGINE "IndexRow;",
    this->env);
  if (find_flag == HA_READ_PREFIX_LAST_OR_PREV)
  {
    find_flag = HA_READ_KEY_OR_PREV;
  }

  KEY *key_info = table->s->key_info + this->active_index;
  KEY_PART_INFO *key_part = key_info->key_part;
  KEY_PART_INFO *end_key_part = key_part + key_info->key_parts;
  key_part_map counter = keypart_map;
  int key_count = 0;
  while(counter)
  {
    counter >>= 1;
    key_count++;
  }

  uchar* key_copies[key_count];
  uint key_sizes[key_count];
  jboolean key_null_bits[key_count];
  jboolean key_is_null[key_count];
  const char* key_names[key_count];
  memset(key_null_bits, JNI_FALSE, key_count);
  memset(key_is_null, JNI_FALSE, key_count);
  memset(key_copies, 0, key_count);
  uchar* key_iter = (uchar*)key;
  int index = 0;

  while (key_part < end_key_part && keypart_map)
  {
    Field* field = key_part->field;
    key_names[index] = field->field_name;
    uint store_length = key_part->store_length;
    uint offset = store_length;
    if (this->is_field_nullable(this->table_name(), field->field_name))
    {
      if(key_iter[0] == 1)
      {
        if(index == (key_count - 1) && find_flag == HA_READ_AFTER_KEY)
        {
          key_is_null[index] = JNI_FALSE;
          DBUG_RETURN(index_first(buf));
        }
        else
        {
          key_is_null[index] = JNI_TRUE;
        }
      }

      // If the index is nullable, then the first byte is the null flag.  Ignore it.
      key_iter++;
      offset--;
      key_null_bits[index] = JNI_TRUE;
      store_length--;
    }

    uchar* key_copy = create_key_copy(field, key_iter, &store_length, table->in_use);
    key_sizes[index] = store_length;
    key_copies[index] = key_copy;
    keypart_map >>= 1;
    key_part++;
    key_iter += offset;
    index++;
  }

  jobject key_values = create_key_value_list(index, key_sizes, key_copies, key_names, key_null_bits, key_is_null);
  for (int x = 0; x < index; x++)
  {
    ARRAY_DELETE(key_copies[x]);
  }

  jobject java_find_flag = find_flag_to_java(find_flag, this->env);
  jobject index_row = this->env->CallStaticObjectMethod(adapter_class, index_read_method, this->curr_scan_id, key_values, java_find_flag);
  DBUG_RETURN(read_index_row(index_row, buf));
}

int CloudHandler::index_first(uchar *buf)
{
  DBUG_ENTER("CloudHandler::index_first");
  DBUG_RETURN(get_index_row("INDEX_FIRST", buf));
}

int CloudHandler::index_last(uchar *buf)
{
  DBUG_ENTER("CloudHandler::index_last");
  DBUG_RETURN(get_index_row("INDEX_LAST", buf));
}

int CloudHandler::get_next_index_row(uchar* buf)
{
  jclass adapter_class = this->adapter();
  jmethodID index_next_method = find_static_method(adapter_class, "nextIndexRow", "(J)L" MYSQLENGINE "IndexRow;",this->env);
  jobject index_row = this->env->CallStaticObjectMethod(adapter_class, index_next_method, this->curr_scan_id);

  return read_index_row(index_row, buf); 
}

int CloudHandler::get_index_row(const char* indexType, uchar* buf)
{
  jclass adapter_class = this->adapter();
  jmethodID index_read_method = find_static_method(adapter_class, "indexRead", "(JLjava/util/List;L" MYSQLENGINE "IndexReadType;)L" MYSQLENGINE "IndexRow;",this->env);
  jclass read_class = find_jni_class("IndexReadType", this->env);
  jfieldID field_id = this->env->GetStaticFieldID(read_class, indexType, "L" MYSQLENGINE "IndexReadType;");
  jobject java_find_flag = this->env->GetStaticObjectField(read_class, field_id);
  jobject index_row = this->env->CallStaticObjectMethod(adapter_class, index_read_method, this->curr_scan_id, NULL, java_find_flag);
  return read_index_row(index_row, buf); 
}

int CloudHandler::read_index_row(jobject index_row, uchar* buf)
{
  jclass index_row_class = find_jni_class("IndexRow", this->env);
  jmethodID get_uuid_method = this->env->GetMethodID(index_row_class, "getUUID",
      "()[B");
  jmethodID get_rowmap_method = this->env->GetMethodID(index_row_class,
      "getRowMap", "()Ljava/util/Map;");

  jobject rowMap = this->env->CallObjectMethod(index_row, get_rowmap_method);
  if (rowMap == NULL)
  {
    this->table->status = STATUS_NOT_FOUND;
    return HA_ERR_END_OF_FILE;
  }

  this->store_uuid_ref(index_row, get_uuid_method);

  this->java_to_sql(buf, rowMap);

  this->table->status = 0;
  return 0;
}

void CloudHandler::position(const uchar *record)
{
  DBUG_ENTER("CloudHandler::position");
  DBUG_VOID_RETURN;
}

int CloudHandler::rnd_pos(uchar *buf, uchar *pos)
{
  int rc = 0;
  ha_statistic_increment(&SSV::ha_read_rnd_count); // Boilerplate
  DBUG_ENTER("CloudHandler::rnd_pos");

  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str, FALSE);

  jclass adapter_class = this->adapter();
  jmethodID get_row_method = find_static_method(adapter_class, "getRow", "(J[B)L" MYSQLENGINE "Row;",this->env);
  jbyteArray uuid = convert_value_to_java_bytes(pos, 16, this->env);
  jobject row = this->env->CallStaticObjectMethod(adapter_class, get_row_method,
      this->curr_scan_id, uuid);

  jclass row_class = find_jni_class("Row", this->env);
  jmethodID get_row_map_method = this->env->GetMethodID(row_class, "getRowMap",
      "()Ljava/util/Map;");

  jobject row_map = this->env->CallObjectMethod(row, get_row_map_method);

  if (row_map == NULL)
  {
    this->table->status = STATUS_NOT_FOUND;
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }

  java_to_sql(buf, row_map);
  this->table->status = 0;

  MYSQL_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}

int CloudHandler::rnd_end()
{
  DBUG_ENTER("CloudHandler::rnd_end");

  this->end_scan();
  this->detach_thread();
  this->reset_scan_counter();

  DBUG_RETURN(0);
}

int CloudHandler::index_next(uchar *buf)
{
  DBUG_ENTER("CloudHandler::index_next");
  DBUG_RETURN(this->retrieve_value_from_index(buf));
}

int CloudHandler::index_prev(uchar *buf)
{
  DBUG_ENTER("CloudHandler::index_prev");
  DBUG_RETURN(this->retrieve_value_from_index(buf));
}

int CloudHandler::retrieve_value_from_index(uchar* buf)
{
  int rc = 0;

  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str, TRUE);
  my_bitmap_map * orig_bitmap = dbug_tmp_use_all_columns(table, table->read_set);

  rc = get_next_index_row(buf);

  dbug_tmp_restore_column_map(table->read_set, orig_bitmap);
  MYSQL_READ_ROW_DONE(rc);

  return rc;
}

int CloudHandler::rnd_init(bool scan)
{
  DBUG_ENTER("CloudHandler::rnd_init");

  attach_thread();

  jclass adapter_class = this->adapter();
  jmethodID start_scan_method = find_static_method(adapter_class, "startScan", "(Ljava/lang/String;Z)J",this->env);
  jstring table_name = this->table_name();

  jboolean java_scan_boolean = scan ? JNI_TRUE : JNI_FALSE;

  this->curr_scan_id = this->env->CallStaticLongMethod(adapter_class,
      start_scan_method, table_name, java_scan_boolean);

  this->performing_scan = scan;

  DBUG_RETURN(0);
}

int CloudHandler::rnd_next(uchar *buf)
{
  int rc = 0;

  ha_statistic_increment(&SSV::ha_read_rnd_next_count);
  DBUG_ENTER("CloudHandler::rnd_next");

  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str, TRUE);

  memset(buf, 0, table->s->null_bytes);
  jclass adapter_class = this->adapter();
  jmethodID next_row_method = find_static_method(adapter_class, "nextRow", "(J)L" MYSQLENGINE "Row;",this->env);
  jobject row = this->env->CallStaticObjectMethod(adapter_class,
      next_row_method, this->curr_scan_id);

  jclass row_class = find_jni_class("Row", this->env);
  jmethodID get_row_map_method = this->env->GetMethodID(row_class, "getRowMap",
      "()Ljava/util/Map;");
  jmethodID get_uuid_method = this->env->GetMethodID(row_class, "getUUID",
      "()[B");

  jobject row_map = this->env->CallObjectMethod(row, get_row_map_method);

  if (row_map == NULL)
  {
    this->table->status = STATUS_NOT_FOUND;
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }

  this->store_uuid_ref(row, get_uuid_method);
  java_to_sql(buf, row_map);
  this->table->status = 0;

  MYSQL_READ_ROW_DONE(rc);

  DBUG_RETURN(rc);
}

void CloudHandler::end_scan()
{
  jclass adapter_class = this->adapter();
  jmethodID end_scan_method = find_static_method(adapter_class, "endScan", "(J)V",this->env);
  this->env->CallStaticVoidMethod(adapter_class, end_scan_method, this->curr_scan_id);
}

void CloudHandler::reset_index_scan_counter()
{
  this->curr_scan_id = -1;
  this->active_index = -1;
}

void CloudHandler::reset_scan_counter()
{
  this->curr_scan_id = -1;
  this->performing_scan = false;
}
