#include "HoneycombHandler.h"
#include "JNICache.h"

int HoneycombHandler::index_init(uint idx, bool sorted)
{
  DBUG_ENTER("HoneycombHandler::index_init");
  JavaFrame frame(env);

  this->active_index = idx;

  KEY *pos = table->s->key_info + idx;
  KEY_PART_INFO *key_part = pos->key_part;
  KEY_PART_INFO *key_part_end = key_part + pos->key_parts;
  const char* column_names = this->index_name(key_part, key_part_end,
      pos->key_parts);

  jclass adapter_class = cache->hbase_adapter().clazz;
  jmethodID start_scan_method = cache->hbase_adapter().start_index_scan;
  jstring table_name = this->table_name();
  jstring java_column_names = this->string_to_java_string(column_names);

  this->curr_scan_id = this->env->CallStaticLongMethod(adapter_class,
      start_scan_method, table_name, java_column_names);
  ARRAY_DELETE(column_names);


  DBUG_RETURN(0);
}

int HoneycombHandler::index_end()
{
  DBUG_ENTER("HoneycombHandler::index_end");

  this->end_scan();

  DBUG_RETURN(0);
}

jobject HoneycombHandler::create_key_value_list(int index, uint* key_sizes,
    uchar** key_copies, const char** key_names, jboolean* key_null_bits,
    jboolean* key_is_null)
{
  jobject key_values = env->NewObject(cache->linked_list().clazz,
      cache->linked_list().init);
  JavaFrame frame(env, 3*index);
  for(int x = 0; x < index; x++)
  {
    jbyteArray java_key = this->env->NewByteArray(key_sizes[x]);
    this->env->SetByteArrayRegion(java_key, 0, key_sizes[x], (jbyte*) key_copies[x]);
    jstring key_name = string_to_java_string(key_names[x]);
    jobject key_value = this->env->NewObject(cache->key_value().clazz,
        cache->key_value().init, key_name, java_key, key_null_bits[x],
        key_is_null[x]);
    env->CallObjectMethod(key_values, cache->linked_list().add, key_value);
  }
  return key_values;
}

int HoneycombHandler::index_read_map(uchar * buf, const uchar * key,
    key_part_map keypart_map, enum ha_rkey_function find_flag)
{
  DBUG_ENTER("HoneycombHandler::index_read_map");
  jclass adapter_class = cache->hbase_adapter().clazz;
  jmethodID index_read_method = cache->hbase_adapter().index_read;
  if (find_flag == HA_READ_PREFIX_LAST_OR_PREV)
  {
    find_flag = HA_READ_KEY_OR_PREV;
  }

  KEY *key_info = table->s->key_info + this->active_index;
  KEY_PART_INFO *key_part = key_info->key_part;
  KEY_PART_INFO *end_key_part = key_part + key_info->key_parts;
  int key_count = 0;
  if (keypart_map == HA_WHOLE_KEY)
  {
    key_count = key_info->key_parts;
  }
  else
  {
    key_part_map counter = keypart_map;
    while(counter)
    {
      counter >>= 1;
      key_count++;
    }
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
          for (int x = 0; x < index; x++)
          {
            ARRAY_DELETE(key_copies[x]);
          }
          DBUG_RETURN(index_first(buf));
        }
        else
        {
          key_is_null[index] = JNI_TRUE;
        }
      }

      // If the index is nullable, then the first byte is the null flag.
      // Ignore it.
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

  JavaFrame frame(env);
  jobject key_values = create_key_value_list(index, key_sizes, key_copies,
      key_names, key_null_bits, key_is_null);
  for (int x = 0; x < index; x++)
  {
    ARRAY_DELETE(key_copies[x]);
  }

  jobject java_find_flag = env->GetStaticObjectField(cache->index_read_type().clazz,
      find_flag_to_java(find_flag, cache));
  jobject index_row = this->env->CallStaticObjectMethod(adapter_class,
      index_read_method, this->curr_scan_id, key_values, java_find_flag);
  int rc = read_index_row(index_row, buf);

  DBUG_RETURN(rc);
}

int HoneycombHandler::index_first(uchar *buf)
{
  DBUG_ENTER("HoneycombHandler::index_first");
  DBUG_RETURN(get_index_row(cache->index_read_type().INDEX_FIRST, buf));
}

int HoneycombHandler::index_last(uchar *buf)
{
  DBUG_ENTER("HoneycombHandler::index_last");
  DBUG_RETURN(get_index_row(cache->index_read_type().INDEX_LAST, buf));
}

int HoneycombHandler::get_next_index_row(uchar* buf)
{
  JavaFrame frame(env);
  jclass adapter_class = cache->hbase_adapter().clazz;
  jmethodID index_next_method = cache->hbase_adapter().next_index_row;
  jobject index_row = this->env->CallStaticObjectMethod(adapter_class,
      index_next_method, this->curr_scan_id);
  int rc = read_index_row(index_row, buf);
  return rc;
}

int HoneycombHandler::get_index_row(jfieldID field_id, uchar* buf)
{
  JavaFrame frame(env);
  jclass adapter_class = cache->hbase_adapter().clazz;
  jmethodID index_read_method = cache->hbase_adapter().index_read;
  jclass read_class = cache->index_read_type().clazz;
  jobject java_find_flag = this->env->GetStaticObjectField(read_class, field_id);
  jobject index_row = this->env->CallStaticObjectMethod(adapter_class,
      index_read_method, this->curr_scan_id, NULL, java_find_flag);
  int rc = read_index_row(index_row, buf);
  return rc;
}

int HoneycombHandler::read_index_row(jobject index_row, uchar* buf)
{
  jclass index_row_class = cache->index_row().clazz;
  jmethodID get_uuid_method = cache->index_row().get_uuid;
  jmethodID get_row_map_method = cache->index_row().get_row_map;

  jobject rowMap = this->env->CallObjectMethod(index_row, get_row_map_method);
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

void HoneycombHandler::position(const uchar *record)
{
  DBUG_ENTER("HoneycombHandler::position");
  DBUG_VOID_RETURN;
}

int HoneycombHandler::rnd_pos(uchar *buf, uchar *pos)
{
  int rc = 0;
  ha_statistic_increment(&SSV::ha_read_rnd_count); // Boilerplate
  DBUG_ENTER("HoneycombHandler::rnd_pos");

  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str, FALSE);

  jclass adapter_class = cache->hbase_adapter().clazz;
  jmethodID get_row_method = cache->hbase_adapter().get_row;
  jbyteArray uuid = convert_value_to_java_bytes(pos, 16, this->env);
  jobject row = this->env->CallStaticObjectMethod(adapter_class, get_row_method,
      this->curr_scan_id, uuid);

  jclass row_class = cache->row().clazz;
  jmethodID get_row_map_method = cache->index_row().get_row_map;

  jobject row_map = this->env->CallObjectMethod(row, get_row_map_method);

  if (row_map == NULL)
  {
    this->table->status = STATUS_NOT_FOUND;
    rc = HA_ERR_END_OF_FILE;
    goto cleanup;
  }

  java_to_sql(buf, row_map);
  this->table->status = 0;

  MYSQL_READ_ROW_DONE(rc);
cleanup:
  DELETE_REF(env, row);
  DELETE_REF(env, uuid);

  DBUG_RETURN(rc);
}

int HoneycombHandler::rnd_end()
{
  DBUG_ENTER("HoneycombHandler::rnd_end");

  this->end_scan();

  DBUG_RETURN(0);
}

int HoneycombHandler::index_next(uchar *buf)
{
  DBUG_ENTER("HoneycombHandler::index_next");
  DBUG_RETURN(this->retrieve_value_from_index(buf));
}

int HoneycombHandler::index_prev(uchar *buf)
{
  DBUG_ENTER("HoneycombHandler::index_prev");
  DBUG_RETURN(this->retrieve_value_from_index(buf));
}

int HoneycombHandler::retrieve_value_from_index(uchar* buf)
{
  int rc = 0;

  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str, TRUE);
  my_bitmap_map * orig_bitmap = dbug_tmp_use_all_columns(table, table->read_set);

  rc = get_next_index_row(buf);

  dbug_tmp_restore_column_map(table->read_set, orig_bitmap);
  MYSQL_READ_ROW_DONE(rc);

  return rc;
}

int HoneycombHandler::rnd_init(bool scan)
{
  DBUG_ENTER("HoneycombHandler::rnd_init");

  JavaFrame frame(env);
  jclass adapter_class = cache->hbase_adapter().clazz;
  jmethodID start_scan_method = cache->hbase_adapter().start_scan;
  jstring table_name = this->table_name();

  jboolean java_scan_boolean = scan ? JNI_TRUE : JNI_FALSE;

  this->curr_scan_id = this->env->CallStaticLongMethod(adapter_class,
      start_scan_method, table_name, java_scan_boolean);

  this->performing_scan = scan;

  DBUG_RETURN(0);
}

int HoneycombHandler::rnd_next(uchar *buf)
{
  int rc = 0;

  ha_statistic_increment(&SSV::ha_read_rnd_next_count);
  DBUG_ENTER("HoneycombHandler::rnd_next");

  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str, TRUE);
  JavaFrame frame(env);

  memset(buf, 0, table->s->null_bytes);
  jclass adapter_class = cache->hbase_adapter().clazz;
  jmethodID next_row_method = cache->hbase_adapter().next_row;
  jobject row = this->env->CallStaticObjectMethod(adapter_class,
      next_row_method, this->curr_scan_id);

  jclass row_class = cache->row().clazz;
  jmethodID get_uuid_method = cache->index_row().get_uuid;
  jmethodID get_row_map_method = cache->index_row().get_row_map;

  jobject row_map = this->env->CallObjectMethod(row, get_row_map_method);

  if (row_map == NULL)
  {
    this->table->status = STATUS_NOT_FOUND;
    rc = HA_ERR_END_OF_FILE;
    DBUG_RETURN(rc);
  }

  this->store_uuid_ref(row, get_uuid_method);
  java_to_sql(buf, row_map);
  this->table->status = 0;

  MYSQL_READ_ROW_DONE(rc);

  DBUG_RETURN(rc);
}

void HoneycombHandler::end_scan()
{
  if(scan_ids_count == scan_ids_length)
  {
    long long* old = scan_ids;
    scan_ids_length *= 2;
    scan_ids = new long long[scan_ids_length];
    memset(scan_ids, 0, scan_ids_length);
    memcpy(scan_ids, old, (scan_ids_count - 1) * sizeof(long long));
    ARRAY_DELETE(old);
  }

  scan_ids[scan_ids_count++] = this->curr_scan_id;
}
