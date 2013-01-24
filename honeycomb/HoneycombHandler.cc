#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation        // gcc: Class implementation
#endif

#include "HoneycombHandler.h"

const char **HoneycombHandler::bas_ext() const
{
  static const char *honeycomb_exts[] = { NullS };
  return honeycomb_exts;
}

HoneycombHandler::HoneycombHandler(handlerton *hton, TABLE_SHARE *table_arg,
    mysql_mutex_t* mutex, HASH* open_tables, JavaVM* jvm, JNICache* cache)
: handler(hton, table_arg), jvm(jvm), honeycomb_mutex(mutex),
  honeycomb_open_tables(open_tables), cache(cache)
{
  attach_thread(this->jvm, this->env);
  this->ref_length = 16;
  this->rows_written = 0;
  this->failed_key_index = 0;
  this->scan_ids_length = 32;
  this->scan_ids_count = 0;
  this->scan_ids = new long long[this->scan_ids_length];
  this->curr_write_id = -1;
  memset(scan_ids, 0, scan_ids_length);
}

HoneycombHandler::~HoneycombHandler()
{
  jclass adapter_class = cache->hbase_adapter().clazz;
  jmethodID end_scan_method = cache->hbase_adapter().end_scan;
  for (int i = 0; i < this->scan_ids_count; i++)
  {
    this->env->CallStaticVoidMethod(adapter_class, end_scan_method, scan_ids[i]);
  }
  ARRAY_DELETE(this->scan_ids);
  this->flush_writes();
  detach_thread(this->jvm);
}

void HoneycombHandler::release_auto_increment()
{
  // Stored functions call this last. Hack to get around MySQL not calling
  // start/end bulk insert on insert in a stored function.
  this->flush_writes();
}

int HoneycombHandler::open(const char *path, int mode, uint test_if_locked)
{
  DBUG_ENTER("HoneycombHandler::open");

  if (!(share = get_share(path, table)))
  {
    DBUG_RETURN(1);
  }

  thr_lock_data_init(&share->lock, &lock, (void*) this);

  DBUG_RETURN(0);
}

int HoneycombHandler::close(void)
{
  DBUG_ENTER("HoneycombHandler::close");

  DBUG_RETURN(free_share(share));
}

void HoneycombHandler::store_field_value(Field *field, char *val, int val_length)
{
  int type = field->real_type();

  if (!is_unsupported_field(type))
  {
    if (is_integral_field(type))
    {
      if(type == MYSQL_TYPE_LONGLONG)
      {
        memcpy(field->ptr, val, sizeof(ulonglong));
        if (is_little_endian())
        {
          reverse_bytes(field->ptr, val_length);
        }
      }
      else
      {
        long long long_value = *(long long*) val;
        if (is_little_endian())
        {
          long_value = bswap64(long_value);
        }

        field->store(long_value, false);
      }
    }
    else if (is_byte_field(type))
    {
      field->store(val, val_length, &my_charset_bin);
    }
    else if (is_date_or_time_field(type))
    {
      if (type == MYSQL_TYPE_TIME)
      {
        long long long_value = *(long long*) val;
        if (is_little_endian())
        {
          long_value = bswap64(long_value);
        }
        field->store(long_value, false);
      }
      else
      {
        MYSQL_TIME mysql_time;
        int was_cut;
        str_to_datetime(val, val_length, &mysql_time, TIME_FUZZY_DATE, &was_cut);
        field->store_time(&mysql_time, mysql_time.time_type);
      }
    }
    else if (is_decimal_field(type))
    {
      // TODO: Is this reliable? Field_decimal doesn't seem to have these members.
      // Potential crash for old decimal types. - ABC
      uint precision = ((Field_new_decimal*) field)->precision;
      uint scale = ((Field_new_decimal*) field)->dec;
      my_decimal decimal_val;
      binary2my_decimal(0, (const uchar *) val, &decimal_val, precision, scale);
      ((Field_new_decimal *) field)->store_value(
          (const my_decimal*) &decimal_val);
    }
    else if (is_floating_point_field(type))
    {
      double double_value;
      if (is_little_endian())
      {
        long long* long_ptr = (long long*) val;
        longlong swapped_long = bswap64(*long_ptr);
        double_value = *(double*) &swapped_long;
      } else
      {
        double_value = *(double*) val;
      }
      field->store(double_value);
    }
  }
}

void HoneycombHandler::java_to_sql(uchar* buf, jobject row_map)
{
  jboolean is_copy = JNI_FALSE;
  my_bitmap_map *orig_bitmap;
  orig_bitmap = dbug_tmp_use_all_columns(table, table->write_set);
  JavaFrame frame(env, table->s->fields + 1);

  for (int i = 0; i < table->s->fields; i++)
  {
    Field *field = table->field[i];
    const char* key = field->field_name;
    jstring java_key = string_to_java_string(key);
    jbyteArray java_val = java_map_get(row_map, java_key, this->env);
    if (java_val == NULL)
    {
      field->set_null();
      continue;
    }
    char* val = (char*) this->env->GetByteArrayElements(java_val, &is_copy);
    jsize val_length = this->env->GetArrayLength(java_val);

    my_ptrdiff_t offset = (my_ptrdiff_t) (buf - this->table->record[0]);
    field->move_field_offset(offset);

    field->set_notnull(); // for some reason the field was inited as null during rnd_pos
    this->store_field_value(field, val, val_length);

    field->move_field_offset(-offset);
    this->env->ReleaseByteArrayElements(java_val, (jbyte*) val, 0);
  }

  dbug_tmp_restore_column_map(table->write_set, orig_bitmap);
}

int HoneycombHandler::external_lock(THD *thd, int lock_type)
{
  DBUG_ENTER("HoneycombHandler::external_lock");
  jclass adapter_class = cache->hbase_adapter().clazz;
  if (lock_type == F_WRLCK || lock_type == F_RDLCK)
  {
    jmethodID start_write_method = cache->hbase_adapter().start_write;
    jlong write_id = this->env->CallStaticLongMethod(adapter_class,
        start_write_method);
    this->curr_write_id = write_id;
  }

  if (lock_type == F_UNLCK)
  {
    jmethodID update_count_method = cache->hbase_adapter().increment_row_count;
    jstring table_name = this->table_name();
    this->env->CallStaticVoidMethod(adapter_class, update_count_method,
        table_name, (jlong) this->rows_written);
    DELETE_REF(env, table_name);
    this->rows_written = 0;
    jmethodID end_write_method = cache->hbase_adapter().end_write;
    this->env->CallStaticVoidMethod(adapter_class, end_write_method,
        (jlong)this->curr_write_id);
    this->curr_write_id = -1;
  }
  DBUG_RETURN(0);
}

char* HoneycombHandler::index_name(TABLE* table, uint key)
{
    KEY *pos = table->key_info + key;
    KEY_PART_INFO *key_part = pos->key_part;
    KEY_PART_INFO *key_part_end = key_part + pos->key_parts;
    return this->index_name(key_part, key_part_end, pos->key_parts);
}

char* HoneycombHandler::index_name(KEY_PART_INFO* key_part,
    KEY_PART_INFO* key_part_end, uint key_parts)
{
    size_t size = 0;

    KEY_PART_INFO* start = key_part;
    for (; key_part != key_part_end; key_part++)
    {
      Field *field = key_part->field;
      size += strlen(field->field_name);
    }

    key_part = start;
    char* name = new char[size + key_parts];
    memset(name, 0, size + key_parts);
    for (; key_part != key_part_end; key_part++)
    {
      Field *field = key_part->field;
      strcat(name, field->field_name);
      if ((key_part+1) != key_part_end)
      {
        strcat(name, ",");
      }
    }

    return name;
}

THR_LOCK_DATA **HoneycombHandler::store_lock(THD *thd, THR_LOCK_DATA **to,
    enum thr_lock_type lock_type)
{
  if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK)
    lock.type = lock_type;
  *to++ = &lock;
  return to;
}

/*
 Free lock controls.
 */
int HoneycombHandler::free_share(HoneycombShare *share)
{
  DBUG_ENTER("HoneycombHandler::free_share");
  mysql_mutex_lock(honeycomb_mutex);
  int result_code = 0;
  if (!--share->use_count)
  {
    my_hash_delete(honeycomb_open_tables, (uchar*) share);
    thr_lock_delete(&share->lock);
    my_free(share);
  }

  mysql_mutex_unlock(honeycomb_mutex);

  DBUG_RETURN(result_code);
}

ha_rows HoneycombHandler::records_in_range(uint inx, key_range *min_key,
    key_range *max_key)
{
  return stats.records;
}

// MySQL calls this function all over the place whenever it needs you to update
// some crucial piece of info. It expects you to use this to set information
// about your indexes and error codes, as well as general info about your engine.
// The bit flags (defined in my_base.h) passed in will vary depending on what
// it wants you to update during this call. - ABC
int HoneycombHandler::info(uint flag)
{
  // TODO: Update this function to take into account the flag being passed in,
  // like the other engines
  ha_rows rec_per_key;

  DBUG_ENTER("HoneycombHandler::info");
  if (flag & HA_STATUS_VARIABLE)
  {
    JavaFrame frame(env);
    jclass adapter_class = cache->hbase_adapter().clazz;
    jmethodID get_count_method = cache->hbase_adapter().get_row_count;
    jstring table_name = this->table_name();
    jlong row_count = this->env->CallStaticLongMethod(adapter_class,
        get_count_method, table_name);
    if (row_count < 0)
      row_count = 0;
    if (row_count == 0 && !(flag & HA_STATUS_TIME))
      row_count++;

    THD* thd = ha_thd();
    if (thd_sql_command(thd) == SQLCOM_TRUNCATE)
    {
      row_count = 1;
    }

    stats.records = row_count;
    stats.deleted = 0;
    stats.max_data_file_length = this->max_supported_record_length();
    stats.data_file_length = stats.records * this->table->s->reclength;
    stats.index_file_length = this->max_supported_key_length();
    stats.delete_length = stats.deleted * stats.mean_rec_length;
    stats.check_time = 0;

    if (stats.records == 0) {
      stats.mean_rec_length = 0;
    } else {
      stats.mean_rec_length = (ulong) (stats.data_file_length / stats.records);
    }
  }

  if (flag & HA_STATUS_CONST)
  {
    // Update index cardinality - see ::analyze() function for more explanation
    /* Since MySQL seems to favor table scans
       too much over index searches, we pretend
       index selectivity is 2 times better than
       our estimate: */

    for (int i = 0; i < this->table->s->keys; i++)
    {
      for (int j = 0; j < table->key_info[i].key_parts; j++)
      {
        rec_per_key = stats.records / 2;

        if (rec_per_key == 0) {
          rec_per_key = 1;
        }

        table->key_info[i].rec_per_key[j] = rec_per_key >= ~(ulong) 0 ?
          ~(ulong) 0 : (ulong) rec_per_key;
      }
    }
  }
  // MySQL needs us to tell it the index of the key which caused the last
  // operation to fail Should be saved in this->failed_key_index for now
  // Later, when we implement transactions, we should use this opportunity to
  // grab the info from the trx itself.
  if (flag & HA_STATUS_ERRKEY)
  {
    this->errkey = this->failed_key_index;
    this->failed_key_index = -1;
  }
  if ((flag & HA_STATUS_AUTO) && table->found_next_number_field) {
    JavaFrame frame(env);
    jclass adapter_class = cache->hbase_adapter().clazz;
    jmethodID get_autoincrement_value_method = cache->hbase_adapter().get_autoincrement_value;
    jlong autoincrement_value = (jlong) this->env->CallStaticObjectMethod(adapter_class,
        get_autoincrement_value_method, this->table_name(),
        string_to_java_string(table->found_next_number_field->field_name));
    stats.auto_increment_value = (ulonglong) autoincrement_value;
  }
  DBUG_RETURN(0);
}

HoneycombShare *HoneycombHandler::get_share(const char *table_name, TABLE *table)
{
  HoneycombShare *share;
  char *tmp_path_name;
  uint path_length;

  mysql_mutex_lock(honeycomb_mutex);
  path_length = (uint) strlen(table_name);

  /*
   If share is not present in the hash, create a new share and
   initialize its members.
   */
  if (!(share = (HoneycombShare*) my_hash_search(honeycomb_open_tables,
      (uchar*) table_name, path_length)))
  {
    if (!my_multi_malloc(MYF(MY_WME | MY_ZEROFILL), &share, sizeof(*share),
        &tmp_path_name, path_length + 1, NullS))
    {
      mysql_mutex_unlock(honeycomb_mutex);
      return NULL;
    }
  }

  share->use_count = 0;
  share->table_path_length = path_length;
  share->path_to_table = tmp_path_name;
  share->crashed = FALSE;
  share->rows_recorded = 0;

  if (my_hash_insert(honeycomb_open_tables, (uchar*) share))
    goto error;
  thr_lock_init(&share->lock);

  share->use_count++;
  mysql_mutex_unlock(honeycomb_mutex);

  return share;

  error:
  mysql_mutex_unlock(honeycomb_mutex);
  my_free(share);

  return NULL;
}

int HoneycombHandler::extra(enum ha_extra_function operation)
{
  DBUG_ENTER("HoneycombHandler::extra");
  DBUG_RETURN(0);
}

bool HoneycombHandler::check_if_incompatible_data(HA_CREATE_INFO *create_info,
    uint table_changes)
{
  if (table_changes != IS_EQUAL_YES)
  {

    return (COMPATIBLE_DATA_NO);
  }

  if (this->check_for_renamed_column(table, NULL))
  {
    return COMPATIBLE_DATA_NO;
  }

  /* Check that row format didn't change */
  if ((create_info->used_fields & HA_CREATE_USED_ROW_FORMAT)
      && create_info->row_type != ROW_TYPE_DEFAULT
      && create_info->row_type != get_row_type())
  {

    return (COMPATIBLE_DATA_NO);
  }

  /* Specifying KEY_BLOCK_SIZE requests a rebuild of the table. */
  if (create_info->used_fields & HA_CREATE_USED_KEY_BLOCK_SIZE)
  {
    return (COMPATIBLE_DATA_NO);
  }

  return (COMPATIBLE_DATA_YES);
}

bool HoneycombHandler::check_for_renamed_column(const TABLE* table,
    const char* col_name)
{
  uint k;
  Field* field;

  for (k = 0; k < table->s->fields; k++)
  {
    field = table->field[k];

    if (field->flags & FIELD_IS_RENAMED)
    {

      // If col_name is not provided, return if the field is marked as being renamed.
      if (!col_name)
      {
        return (true);
      }

      // If col_name is provided, return only if names match
      if (my_strcasecmp(system_charset_info, field->field_name, col_name) == 0)
      {
        return (true);
      }
    }
  }

  return (false);
}

void HoneycombHandler::get_auto_increment(ulonglong offset, ulonglong increment,
                                 ulonglong nb_desired_values,
                                 ulonglong *first_value,
                                 ulonglong *nb_reserved_values)
{
  DBUG_ENTER("HoneycombHandler::get_auto_increment");
  jclass adapter_class = cache->hbase_adapter().clazz;
  jmethodID get_auto_increment_method = cache->hbase_adapter().get_next_autoincrement_value;
  jlong increment_value = (jlong) this->env->CallStaticObjectMethod(adapter_class,
      get_auto_increment_method, this->table_name(),
      string_to_java_string(table->next_number_field->field_name));
  *first_value = (ulonglong)increment_value;
  *nb_reserved_values = ULONGLONG_MAX;
  DBUG_VOID_RETURN;
}

int HoneycombHandler::get_failed_key_index(const char *key_name)
{
  if (this->table->s->keys == 0)
  {
    return 0;
  }

  for (uint key = 0; key < this->table->s->keys; key++)
  {
    char* name = index_name(table, key);
    bool are_equal = strcmp(name, key_name) == 0;
    ARRAY_DELETE(name);
    if (are_equal)
    {
      return key;
    }
  }

  return -1;
}

bool HoneycombHandler::field_has_unique_index(Field *field)
{
  for (int i = 0; i < table->s->keys; i++)
  {
    KEY *key_info = table->s->key_info + i;
    KEY_PART_INFO *key_part = key_info->key_part;
    KEY_PART_INFO *end_key_part = key_part + key_info->key_parts;

    while(key_part < end_key_part)
    {
      if ((key_info->flags & HA_NOSAME) && strcmp(key_part->field->field_name,
            field->field_name) == 0)
      {
        return true;
      }

      key_part++;
    }
  }

  return false;
}

jstring HoneycombHandler::string_to_java_string(const char *string)
{
  return this->env->NewStringUTF(string);
}

const char *HoneycombHandler::java_to_string(jstring string)
{
  return this->env->GetStringUTFChars(string, JNI_FALSE);
}

bool HoneycombHandler::is_field_nullable(jstring table_name, const char* field_name)
{
  JavaFrame frame(env);
  jclass adapter_class = cache->hbase_adapter().clazz;
  jmethodID is_nullable_method = cache->hbase_adapter().is_nullable;
  bool result = (bool)this->env->CallStaticBooleanMethod(adapter_class,
      is_nullable_method, table_name, string_to_java_string(field_name));
  return result;
}

void HoneycombHandler::store_uuid_ref(jobject index_row, jmethodID get_uuid_method)
{
  JavaFrame frame(env);
  jbyteArray uuid = (jbyteArray) this->env->CallObjectMethod(index_row, get_uuid_method);
  uchar* pos = (uchar*) this->env->GetByteArrayElements(uuid, JNI_FALSE);
  memcpy(this->ref, pos, this->ref_length);
  this->env->ReleaseByteArrayElements(uuid, (jbyte*) pos, 0);
}

int HoneycombHandler::analyze(THD* thd, HA_CHECK_OPT* check_opt)
{
  DBUG_ENTER("HoneycombHandler::analyze");

  // For each key, just tell MySQL that there is only one value per keypart.
  // This is, in effect, like telling MySQL that all our indexes are unique,
  // and should essentially always be used for lookups.  If you don't do this,
  // the optimizer REALLY tries to do scans, even when they're not ideal. - ABC

  for (int i = 0; i < this->table->s->keys; i++)
  {
    for (int j = 0; j < table->key_info[i].key_parts; j++)
    {
      this->table->key_info[i].rec_per_key[j] = 1;
    }
  }

  DBUG_RETURN(0);
}

ha_rows HoneycombHandler::estimate_rows_upper_bound()
{
  DBUG_ENTER("HoneycombHandler::estimate_rows_upper_bound");
  JavaFrame frame(env);

  jclass adapter_class = cache->hbase_adapter().clazz;
  jmethodID get_count_method = cache->hbase_adapter().get_row_count;
  jstring table_name = this->table_name();
  jlong row_count = this->env->CallStaticLongMethod(adapter_class,
      get_count_method, table_name);

  // Stupid MySQL and its filesort. This must be large enough to filesort when
  // there are less than 2 records.
  DBUG_RETURN(row_count < 2 ? 10 : 2*row_count + 1);
}

void HoneycombHandler::flush_writes()
{
  jclass adapter_class = cache->hbase_adapter().clazz;
  jmethodID end_write_method = cache->hbase_adapter().flush_writes;
  this->env->CallStaticVoidMethod(adapter_class, end_write_method,
      (jlong)this->curr_write_id);
}

jstring HoneycombHandler::table_name()
{
  char* database_name = this->table->s->db.str;
  char* table_name = this->table->s->table_name.str;
  char namespaced_table[strlen(database_name) + strlen(table_name) + 2];
  sprintf(namespaced_table, "%s.%s", database_name, table_name);
  return string_to_java_string(namespaced_table);
}
