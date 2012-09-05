#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation        // gcc: Class implementation
#endif

#include "sql_priv.h"
#include "sql_class.h"           // MYSQL_HANDLERTON_INTERFACE_VERSION
#include "CloudHandler.h"
#include "probes_mysql.h"
#include "sql_plugin.h"
#include "ha_cloud.h"
#include "mysql_time.h"
#include "Util.h"
#include "m_string.h"

#include <sys/time.h>
double real_time(timeval start);
const char **CloudHandler::bas_ext() const
{
  static const char *cloud_exts[] =
  {
    NullS
  };

  return cloud_exts;
}

record_buffer *CloudHandler::create_record_buffer(unsigned int length)
{
  DBUG_ENTER("CloudHandler::create_record_buffer");
  record_buffer *r;
  if (!(r = (record_buffer*) my_malloc(sizeof(record_buffer), MYF(MY_WME))))
  {
    DBUG_RETURN(NULL);
  }

  r->length= (int)length;

  if (!(r->buffer= (uchar*) my_malloc(r->length, MYF(MY_WME))))
  {
    my_free(r);
    DBUG_RETURN(NULL);
  }

  DBUG_RETURN(r);
}

void CloudHandler::destroy_record_buffer(record_buffer *r)
{
  DBUG_ENTER("CloudHandler::destroy_record_buffer");
  my_free(r->buffer);
  my_free(r);
  DBUG_VOID_RETURN;
}

int CloudHandler::open(const char *name, int mode, uint test_if_locked)
{
  DBUG_ENTER("CloudHandler::open");

  if (!(share = get_share(name, table)))
  {
    DBUG_RETURN(1);
  }

  thr_lock_data_init(&share->lock, &lock, (void*) this);

  DBUG_RETURN(0);
}

int CloudHandler::close(void)
{
  DBUG_ENTER("CloudHandler::close");

  destroy_record_buffer(rec_buffer);

  DBUG_RETURN(free_share(share));
}

int CloudHandler::write_row(uchar *buf)
{
  DBUG_ENTER("CloudHandler::write_row");

  if (share->crashed)
    DBUG_RETURN(HA_ERR_CRASHED_ON_USAGE);

  ha_statistic_increment(&SSV::ha_write_count);

  int ret = write_row_helper(buf);

  DBUG_RETURN(ret);
}

/*
  This will be called in a table scan right before the previous ::rnd_next()
  call.
*/
int CloudHandler::update_row(const uchar *old_data, uchar *new_data)
{
  DBUG_ENTER("CloudHandler::update_row");

  ha_statistic_increment(&SSV::ha_update_count);

  // TODO: The next two lines should really be some kind of transaction.
  delete_row_helper();
  write_row_helper(new_data);

  jclass adapter_class = this->adapter();
  jmethodID end_write_method = this->env->GetStaticMethodID(adapter_class, "flushWrites", "()V");
  this->env->CallStaticVoidMethod(adapter_class, end_write_method);

  DBUG_RETURN(0);
}

int CloudHandler::delete_row(const uchar *buf)
{
  DBUG_ENTER("CloudHandler::delete_row");
  ha_statistic_increment(&SSV::ha_delete_count);
  delete_row_helper();
  DBUG_RETURN(0);
}

bool CloudHandler::start_bulk_delete()
{
  DBUG_ENTER("CloudHandler::start_bulk_delete");

  attach_thread();

  DBUG_RETURN(true);
}

int CloudHandler::end_bulk_delete()
{
  DBUG_ENTER("CloudHandler::end_bulk_delete");

  detach_thread();

  DBUG_RETURN(0);
}

int CloudHandler::delete_all_rows()
{
  DBUG_ENTER("CloudHandler::delete_all_rows");

  attach_thread();

  jstring tableName = string_to_java_string(this->table->alias);
  jclass adapter_class = this->adapter();
  jmethodID delete_rows_method = this->env->GetStaticMethodID(adapter_class, "deleteAllRows", "(Ljava/lang/String;)I");

  jboolean result = this->env->CallStaticIntMethod(adapter_class, delete_rows_method, tableName);

  detach_thread();

  DBUG_RETURN(0);
}

int CloudHandler::truncate()
{
  DBUG_ENTER("CloudHandler::truncate");

  DBUG_RETURN(delete_all_rows());
}

void CloudHandler::drop_table(const char *name)
{
  close();

  delete_table(name);
}

int CloudHandler::delete_table(const char *name)
{
  DBUG_ENTER("CloudHandler::delete_table");

  attach_thread();

  const char *alias = extract_table_name_from_path(name);

  jstring tableName = string_to_java_string(alias);
  jclass adapter_class = this->adapter();
  jmethodID drop_table_method = this->env->GetStaticMethodID(adapter_class, "dropTable", "(Ljava/lang/String;)Z");

  jboolean result = this->env->CallStaticBooleanMethod(adapter_class, drop_table_method, tableName);

  detach_thread();

  DBUG_RETURN(0);
}

int CloudHandler::delete_row_helper()
{
  DBUG_ENTER("CloudHandler::delete_row_helper");

  jclass adapter_class = this->adapter();
  jmethodID delete_row_method = this->env->GetStaticMethodID(adapter_class, "deleteRow", "(J)Z");
  jlong java_scan_id = curr_scan_id;

  jboolean result = this->env->CallStaticBooleanMethod(adapter_class, delete_row_method, java_scan_id);

  DBUG_RETURN(0);
}

int CloudHandler::rnd_init(bool scan)
{
  DBUG_ENTER("CloudHandler::rnd_init");

  const char* table_name = this->table->alias;

  attach_thread();

  jclass adapter_class = this->adapter();
  jmethodID start_scan_method = this->env->GetStaticMethodID(adapter_class, "startScan", "(Ljava/lang/String;Z)J");
  jstring java_table_name = string_to_java_string(table_name);

  jboolean java_scan_boolean = scan ? JNI_TRUE : JNI_FALSE;

  this->curr_scan_id = this->env->CallStaticLongMethod(adapter_class, start_scan_method, java_table_name, java_scan_boolean);

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
  jlong java_scan_id = curr_scan_id;

  jclass adapter_class = this->adapter();
  jmethodID next_row_method = this->env->GetStaticMethodID(adapter_class, "nextRow", "(J)Lcom/nearinfinity/mysqlengine/jni/Row;");
  jobject row = this->env->CallStaticObjectMethod(adapter_class, next_row_method, java_scan_id);

  jclass row_class = find_jni_class("Row", this->env);
  jmethodID get_row_map_method = this->env->GetMethodID(row_class, "getRowMap", "()Ljava/util/Map;");
  jmethodID get_uuid_method = this->env->GetMethodID(row_class, "getUUID", "()[B");

  jobject row_map = this->env->CallObjectMethod(row, get_row_map_method);

  if (row_map == NULL)
  {
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }

  this->store_uuid_ref(row, get_uuid_method);
  java_to_sql(buf, row_map);

  MYSQL_READ_ROW_DONE(rc);

  DBUG_RETURN(rc);
}

void CloudHandler::java_to_sql(uchar* buf, jobject row_map)
{
  jboolean is_copy = JNI_FALSE;
  my_bitmap_map *orig_bitmap;
  orig_bitmap= dbug_tmp_use_all_columns(table, table->write_set);

  for (int i = 0; i < table->s->fields; i++)
  {
    Field *field = table->field[i];
    field->set_notnull(); // for some reason the field was inited as null during rnd_pos
    const char* key = field->field_name;
    jstring java_key = string_to_java_string(key);
    jbyteArray java_val = java_map_get(row_map, java_key);
    if (java_val == NULL)
    {
      field->set_null();
      continue;
    }
    char* val = (char*) this->env->GetByteArrayElements(java_val, &is_copy);
    jsize val_length = this->env->GetArrayLength(java_val);

    my_ptrdiff_t offset = (my_ptrdiff_t) (buf - this->table->record[0]);
    field->move_field_offset(offset);

    switch (field->real_type())
    {
      case MYSQL_TYPE_TINY:
      case MYSQL_TYPE_SHORT:
      case MYSQL_TYPE_LONG:
      case MYSQL_TYPE_LONGLONG:
      case MYSQL_TYPE_INT24:
      case MYSQL_TYPE_YEAR:
      case MYSQL_TYPE_ENUM:
        {
          long long long_value = *(long long*)val;
          if(this->is_little_endian())
          {
            long_value = __builtin_bswap64(long_value);
          }
          field->store(long_value, false);
          break;
        }
      case MYSQL_TYPE_FLOAT:
      case MYSQL_TYPE_DOUBLE:
        double double_value;
        if (this->is_little_endian())
        {
          long long* long_ptr = (long long*)val;
          longlong swapped_long = __builtin_bswap64(*long_ptr);
          double_value = *(double*)&swapped_long;
        }
        else
        {
          double_value = *(double*)val;
        }
        field->store(double_value);
        break;

      case MYSQL_TYPE_DECIMAL:
      case MYSQL_TYPE_NEWDECIMAL:
        {
          uint precision = ((Field_new_decimal*) field)->precision;
          uint scale = ((Field_new_decimal*) field)->dec;
          my_decimal decimal_val;
          binary2my_decimal(0, (const uchar *) val, &decimal_val, precision, scale);
          ((Field_new_decimal *) field)->store_value((const my_decimal*) &decimal_val);
          decimal_val;
          break;
        }
      case MYSQL_TYPE_TIME:
        {
          MYSQL_TIME mysql_time;
          int warning;
          str_to_time(val, val_length, &mysql_time, &warning);
          field->store_time(&mysql_time, mysql_time.time_type);
          break;
        }
      case MYSQL_TYPE_DATE:
      case MYSQL_TYPE_NEWDATE:
      case MYSQL_TYPE_DATETIME:
      case MYSQL_TYPE_TIMESTAMP:
        {
          MYSQL_TIME mysql_time;
          int was_cut;
          str_to_datetime(val, val_length, &mysql_time, TIME_FUZZY_DATE, &was_cut);
          field->store_time(&mysql_time, mysql_time.time_type);
          break;
        }
      case MYSQL_TYPE_STRING:
      case MYSQL_TYPE_VARCHAR:
      case MYSQL_TYPE_VAR_STRING:
      case MYSQL_TYPE_TINY_BLOB:
      case MYSQL_TYPE_MEDIUM_BLOB:
      case MYSQL_TYPE_BLOB:
      case MYSQL_TYPE_LONG_BLOB:
        field->store(val, val_length, &my_charset_bin);
        break;
      case MYSQL_TYPE_NULL:
      case MYSQL_TYPE_BIT:
      case MYSQL_TYPE_SET:
      case MYSQL_TYPE_GEOMETRY:
      default:
        break;
    }

    field->move_field_offset(-offset);
    this->env->ReleaseByteArrayElements(java_val, (jbyte*)val, 0);
  }

  dbug_tmp_restore_column_map(table->write_set, orig_bitmap);

  return;
}

int CloudHandler::external_lock(THD *thd, int lock_type)
{
  DBUG_ENTER("CloudHandler::external_lock");
  DBUG_RETURN(0);
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
  jmethodID get_row_method = this->env->GetStaticMethodID(adapter_class, "getRow", "(J[B)Lcom/nearinfinity/mysqlengine/jni/Row;");
  jlong java_scan_id = curr_scan_id;
  jbyteArray uuid = convert_value_to_java_bytes(pos, 16);
  jobject row = this->env->CallStaticObjectMethod(adapter_class, get_row_method, java_scan_id, uuid);

  jclass row_class = find_jni_class("Row", this->env);
  jmethodID get_row_map_method = this->env->GetMethodID(row_class, "getRowMap", "()Ljava/util/Map;");

  jobject row_map = this->env->CallObjectMethod(row, get_row_map_method);

  if (row_map == NULL)
  {
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }

  java_to_sql(buf, row_map);

  MYSQL_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}

int CloudHandler::rnd_end()
{
  DBUG_ENTER("CloudHandler::rnd_end");

  jclass adapter_class = this->adapter();
  jmethodID end_scan_method = this->env->GetStaticMethodID(adapter_class, "endScan", "(J)V");
  jlong java_scan_id = curr_scan_id;

  this->env->CallStaticVoidMethod(adapter_class, end_scan_method, java_scan_id);

  detach_thread();

  curr_scan_id = -1;
  this->performing_scan = false;
  DBUG_RETURN(0);
}

void CloudHandler::start_bulk_insert(ha_rows rows)
{
  DBUG_ENTER("CloudHandler::start_bulk_insert");

  attach_thread();
  Logging::info("%d rows to be inserted.", rows);

  DBUG_VOID_RETURN;
}

int CloudHandler::end_bulk_insert()
{
  DBUG_ENTER("CloudHandler::end_bulk_insert");

  jclass adapter_class = this->adapter();
  jmethodID end_write_method = this->env->GetStaticMethodID(adapter_class, "flushWrites", "()V");
  this->env->CallStaticVoidMethod(adapter_class, end_write_method);

  detach_thread();
  DBUG_RETURN(0);
}

int CloudHandler::create(const char *name, TABLE *table_arg,
                         HA_CREATE_INFO *create_info)
{
  DBUG_ENTER("CloudHandler::create");

  attach_thread();

  jclass adapter_class = this->adapter();
  if (adapter_class == NULL)
  {
    print_java_exception(this->env);
    ERROR(("Could not find adapter class HBaseAdapter"));
    DBUG_RETURN(1);
  }

  const char* table_name = create_info->alias;

  jobject columnMap = this->create_java_map();
  FieldMetadata metadata(this->env);

  for (Field **field = table_arg->field ; *field ; field++)
  {
    jobject metadataList = metadata.get_field_metadata(*field, table_arg);
    this->java_map_insert(columnMap, string_to_java_string((*field)->field_name), metadataList);
  }

  jmethodID create_table_method = this->env->GetStaticMethodID(adapter_class, "createTable", "(Ljava/lang/String;Ljava/util/Map;)Z");
  jboolean result = this->env->CallStaticBooleanMethod(adapter_class, create_table_method, string_to_java_string(table_name), columnMap);
  print_java_exception(this->env);

  detach_thread();

  DBUG_RETURN(0);
}

THR_LOCK_DATA **CloudHandler::store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type)
{
  if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK)
    lock.type=lock_type;
  *to++= &lock;
  return to;
}

/*
  Free lock controls.
*/
int CloudHandler::free_share(CloudShare *share)
{
  DBUG_ENTER("CloudHandler::free_share");
  mysql_mutex_lock(cloud_mutex);
  int result_code= 0;
  if (!--share->use_count)
  {
    my_hash_delete(cloud_open_tables, (uchar*) share);
    thr_lock_delete(&share->lock);
    my_free(share);
  }
  mysql_mutex_unlock(cloud_mutex);

  DBUG_RETURN(result_code);
}

int CloudHandler::info(uint)
{
  DBUG_ENTER("CloudHandler::info");
  if (stats.records < 2)
    stats.records= 2;
  DBUG_RETURN(0);
}

CloudShare *CloudHandler::get_share(const char *table_name, TABLE *table)
{
  CloudShare *share;
  char *tmp_path_name;
  uint path_length;

  rec_buffer= create_record_buffer(table->s->reclength);

  if (!rec_buffer)
  {
    DBUG_PRINT("CloudHandler", ("Ran out of memory while allocating record buffer"));

    return NULL;
  }

  mysql_mutex_lock(cloud_mutex);
  path_length=(uint) strlen(table_name);

  /*
  If share is not present in the hash, create a new share and
  initialize its members.
  */
  if (!(share=(CloudShare*) my_hash_search(cloud_open_tables,
              (uchar*) table_name,
              path_length)))
  {
    if (!my_multi_malloc(MYF(MY_WME | MY_ZEROFILL),
                         &share, sizeof(*share),
                         &tmp_path_name, path_length+1,
                         NullS))
    {
      mysql_mutex_unlock(cloud_mutex);
      return NULL;
    }
  }

  share->use_count= 0;
  share->table_path_length= path_length;
  share->path_to_table= tmp_path_name;
  share->crashed= FALSE;
  share->rows_recorded= 0;

  if (my_hash_insert(cloud_open_tables, (uchar*) share))
    goto error;
  thr_lock_init(&share->lock);

  share->use_count++;
  mysql_mutex_unlock(cloud_mutex);

  return share;

error:
  mysql_mutex_unlock(cloud_mutex);
  my_free(share);

  return NULL;
}

int CloudHandler::extra(enum ha_extra_function operation)
{
  DBUG_ENTER("CloudHandler::extra");
  DBUG_RETURN(0);
}

uint32 CloudHandler::max_row_length()
{
  uint32 length = (uint32)(table->s->reclength + table->s->fields*2);

  uint *ptr, *end;
  for (ptr = table->s->blob_field, end=ptr + table->s->blob_fields ; ptr != end ; ptr++)
  {
    if (!table->field[*ptr]->is_null())
      length += 2 + ((Field_blob*)table->field[*ptr])->get_length();
  }

  return length;
}

double real_time(timeval start)
{
  return start.tv_sec + (start.tv_usec / 1000000.0);
}

/* Set up the JNI Environment, and then persist the row to HBase.
 * This helper calls sql_to_java, which returns the row information
 * as a jobject to be sent to the HBaseAdapter.
 */
int CloudHandler::write_row_helper(uchar* buf)
{
  DBUG_ENTER("CloudHandler::write_row_helper");

  jclass adapter_class = this->adapter();
  jmethodID write_row_method = this->env->GetStaticMethodID(adapter_class, "writeRow", "(Ljava/lang/String;Ljava/util/Map;)Z");
  jstring java_table_name = string_to_java_string(this->table->alias);
  jobject java_row_map = sql_to_java();
  this->env->CallStaticBooleanMethod(adapter_class, write_row_method, java_table_name, java_row_map);

  DBUG_RETURN(0);
}

/* Read fields into a java map.
 */
jobject CloudHandler::sql_to_java()
{
  jobject java_map = this->create_java_map();
  // Boilerplate stuff every engine has to do on writes

  if (table->timestamp_field_type & TIMESTAMP_AUTO_SET_ON_INSERT)
    table->timestamp_field->set_time();

  my_bitmap_map *old_map = dbug_tmp_use_all_columns(table, table->read_set);

  uint actualFieldSize;

  for (Field **field_ptr=table->field; *field_ptr; field_ptr++)
  {
    Field * field = *field_ptr;
    jstring field_name = string_to_java_string(field->field_name);

    memset(rec_buffer->buffer, 0, rec_buffer->length);

    const bool is_null = field->is_null();

    if (is_null )
    {
      java_map_insert(java_map, field_name, NULL);
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
        {
          long long integral_value = field->val_int();
          if(this->is_little_endian())
          {
            integral_value = __builtin_bswap64(integral_value);
          }
          actualFieldSize = sizeof integral_value;
          memcpy(rec_buffer->buffer, &integral_value, actualFieldSize);
          break;
        }
      case MYSQL_TYPE_FLOAT:
      case MYSQL_TYPE_DOUBLE:
        {
          double fp_value = field->val_real();
          long long* fp_ptr;
          if(this->is_little_endian())
          {
            fp_ptr = (long long*)&fp_value;
            *fp_ptr = __builtin_bswap64(*fp_ptr);
          }
          actualFieldSize = sizeof fp_value;
          memcpy(rec_buffer->buffer, fp_ptr, actualFieldSize);
          break;
        }
      case MYSQL_TYPE_DECIMAL:
      case MYSQL_TYPE_NEWDECIMAL:
        actualFieldSize = field->key_length();
        memcpy(rec_buffer->buffer, field->ptr, actualFieldSize);
        break;
      case MYSQL_TYPE_DATE:
      case MYSQL_TYPE_NEWDATE:
      case MYSQL_TYPE_TIME:
      case MYSQL_TYPE_DATETIME:
      case MYSQL_TYPE_TIMESTAMP:
        {
          MYSQL_TIME mysql_time;
          char temporal_value[MAX_DATE_STRING_REP_LENGTH];
          field->get_time(&mysql_time);
          my_TIME_to_str(&mysql_time, temporal_value);
          actualFieldSize = strlen(temporal_value);
          memcpy(rec_buffer->buffer, temporal_value, actualFieldSize);
          break;
        }
      case MYSQL_TYPE_STRING:
        {
          char string_value_buff[1024];  // TODO: This is going to cause a buffer overflow for large blob/string types
          String string_value(string_value_buff, sizeof(string_value_buff), &my_charset_bin);
          field->val_str(&string_value);
          actualFieldSize = field->field_length;
          memcpy(rec_buffer->buffer, string_value.ptr(), actualFieldSize);
          break;
        }
      case MYSQL_TYPE_VARCHAR:
      case MYSQL_TYPE_VAR_STRING:
      case MYSQL_TYPE_BLOB:
      case MYSQL_TYPE_TINY_BLOB:
      case MYSQL_TYPE_MEDIUM_BLOB:
      case MYSQL_TYPE_LONG_BLOB:
        {
          char string_value_buff[1024];  // TODO: This is going to cause a buffer overflow for large blob/string types
          String string_value(string_value_buff, sizeof(string_value_buff), &my_charset_bin);
          field->val_str(&string_value);
          actualFieldSize = string_value.length();
          memcpy(rec_buffer->buffer, string_value.ptr(), actualFieldSize);
          break;
        }
      case MYSQL_TYPE_NULL:
      case MYSQL_TYPE_BIT:
      case MYSQL_TYPE_SET:
      case MYSQL_TYPE_GEOMETRY:
      default:
        actualFieldSize = field->field_length;
        memcpy(rec_buffer->buffer, field->ptr, field->field_length);
        break;
    }

    jbyteArray java_bytes = this->convert_value_to_java_bytes(rec_buffer->buffer, actualFieldSize);

    java_map_insert(java_map, field_name, java_bytes);
  }

  dbug_tmp_restore_column_map(table->read_set, old_map);

  return java_map;
}

const char* CloudHandler::java_to_string(jstring j_str)
{
  return this->env->GetStringUTFChars(j_str, NULL);
}

jstring CloudHandler::string_to_java_string(const char *string)
{
  return this->env->NewStringUTF(string);
}

jobject CloudHandler::create_java_map()
{
  jclass map_class = this->env->FindClass("java/util/TreeMap");
  jmethodID constructor = this->env->GetMethodID(map_class, "<init>", "()V");
  return this->env->NewObject(map_class, constructor);
}

jobject CloudHandler::java_map_insert(jobject java_map, jobject key, jobject value)
{
  jclass map_class = this->env->FindClass("java/util/TreeMap");
  jmethodID put_method = this->env->GetMethodID(map_class, "put", "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");

  return this->env->CallObjectMethod(java_map, put_method, key, value);
}

jbyteArray CloudHandler::java_map_get(jobject java_map, jstring key)
{
  jclass map_class = this->env->FindClass("java/util/TreeMap");
  jmethodID get_method = this->env->GetMethodID(map_class, "get", "(Ljava/lang/Object;)Ljava/lang/Object;");

  return (jbyteArray) this->env->CallObjectMethod(java_map, get_method, key);
}

jboolean CloudHandler::java_map_is_empty(jobject java_map)
{
  jclass map_class = this->env->FindClass("java/util/TreeMap");
  jmethodID is_empty_method = this->env->GetMethodID(map_class, "isEmpty", "()Z");
  jboolean result = env->CallBooleanMethod(java_map, is_empty_method);
  return (bool) result;
}

jbyteArray CloudHandler::convert_value_to_java_bytes(uchar* value, uint32 length)
{
  jbyteArray byteArray = this->env->NewByteArray(length);
  jbyte *java_bytes = this->env->GetByteArrayElements(byteArray, 0);

  memcpy(java_bytes, value, length);

  this->env->SetByteArrayRegion(byteArray, 0, length, java_bytes);
  this->env->ReleaseByteArrayElements(byteArray, java_bytes, 0);

  return byteArray;
}

int CloudHandler::index_init(uint idx, bool sorted)
{
  DBUG_ENTER("CloudHandler::index_init");

  this->active_index = idx;

  const char* table_name = this->table->alias;
  const char* column_name = this->table->s->key_info[idx].key_part->field->field_name;
  for (Field **field_ptr=table->field; *field_ptr; field_ptr++)
  {
    Field * field = *field_ptr;
    if (strcmp(field->field_name, column_name) == 0)
    {
      this->index_field = field;
      break;
    }
  }
  attach_thread();

  jclass adapter_class = this->adapter();
  jmethodID start_scan_method = this->env->GetStaticMethodID(adapter_class, "startIndexScan", "(Ljava/lang/String;Ljava/lang/String;)J");
  jstring java_table_name = this->string_to_java_string(table_name);
  jstring java_column_name = this->string_to_java_string(column_name);

  this->curr_scan_id = this->env->CallStaticLongMethod(adapter_class, start_scan_method, java_table_name, java_column_name);

  DBUG_RETURN(0);
}

int CloudHandler::index_end()
{
  DBUG_ENTER("CloudHandler::index_end");

  jclass adapter_class = this->adapter();
  jmethodID end_scan_method = this->env->GetStaticMethodID(adapter_class, "endScan", "(J)V");
  jlong java_scan_id = curr_scan_id;

  this->env->CallStaticVoidMethod(adapter_class, end_scan_method, java_scan_id);

  detach_thread();
  this->curr_scan_id = -1;
  this->active_index = -1;

  DBUG_RETURN(0);
}

int CloudHandler::index_read(uchar *buf, const uchar *key, uint key_len, enum ha_rkey_function find_flag)
{
  DBUG_ENTER("CloudHandler::index_read");

  jclass adapter_class = this->adapter();
  jmethodID index_read_method = this->env->GetStaticMethodID(adapter_class, "indexRead", "(J[BLcom/nearinfinity/mysqlengine/jni/IndexReadType;)Lcom/nearinfinity/mysqlengine/jni/IndexRow;");
  jlong java_scan_id = this->curr_scan_id;
  uchar* key_copy;
  jobject java_find_flag;

  if (find_flag == HA_READ_PREFIX_LAST_OR_PREV)
  {
    find_flag = HA_READ_KEY_OR_PREV;
  }

  if (this->index_field->maybe_null() && is_key_null(key))
  {
    switch (find_flag)
    {
    case HA_READ_KEY_EXACT:
      java_find_flag = java_find_flag_by_name("INDEX_NULL");
      break;
    case HA_READ_AFTER_KEY:
      java_find_flag = java_find_flag_by_name("INDEX_FIRST");
      break;
    default:
      java_find_flag = this->java_find_flag(find_flag);
      break;
    }
  }
  else
  {
    java_find_flag = this->java_find_flag(find_flag);
  }

  if (this->index_field->maybe_null())
  {
    //If the index is nullable, then the first byte tells us whether it is null
    //We can ignore the value by incrementing the pointer and decrementing the length
    key++;
    key_len--;
  }

  int index_field_type = this->index_field->real_type();

  switch(index_field_type)
  {
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_LONGLONG:
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_ENUM:
    {
      key_copy = new uchar[sizeof(long long)]; // Store key as 8 bytes
      const bool is_signed = !is_unsigned_field(this->index_field);
      bytes_to_long(key, key_len, is_signed, key_copy);
      key_len = sizeof(longlong);
      this->make_big_endian(key_copy, key_len);
      break;
    }
    case MYSQL_TYPE_YEAR:
    {
      key_copy = new uchar[sizeof(long long)];

      /* It comes to us as one byte, need to cast it to int and add 1900 */
      uint32_t int_val = (uint32_t)key[0] + 1900;

      bytes_to_long((uchar *)&int_val, sizeof(uint32_t), false, key_copy);
      key_len = sizeof(long long);
      this->make_big_endian(key_copy, key_len);
      break;
    }
    case MYSQL_TYPE_FLOAT:
    {
      double j = (double)floatGet(key);

      key_copy = new uchar[sizeof(double)];
      key_len = sizeof(double);

      doublestore(key_copy, j);
      reverse_bytes(key_copy, key_len);
    }
      break;
    case MYSQL_TYPE_DOUBLE:
    {
      double j;
      doubleget(j, key);

      key_copy = new uchar[sizeof(double)];
      key_len = sizeof(double);

      doublestore(key_copy, j);
      reverse_bytes(key_copy, key_len);
    }
      break;
    case MYSQL_TYPE_DECIMAL:
    case MYSQL_TYPE_NEWDECIMAL:
      {
        key_copy = new uchar[key_len];
        memcpy(key_copy, key, key_len);
        break;
      }
    case MYSQL_TYPE_DATE:
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_TIME:
    case MYSQL_TYPE_TIMESTAMP:
    case MYSQL_TYPE_NEWDATE:
    {
      MYSQL_TIME mysql_time;

      switch (index_field_type)
      {
      case MYSQL_TYPE_DATE:
      case MYSQL_TYPE_NEWDATE:
        if (key_len == 3)
        {
          extract_mysql_newdate((long) uint3korr(key), &mysql_time);
        }
        else
        {
          extract_mysql_old_date((int32) uint4korr(key), &mysql_time);
        }
        break;
      case MYSQL_TYPE_TIMESTAMP:
        extract_mysql_timestamp((long) uint4korr(key), &mysql_time, table->in_use);
        break;
      case MYSQL_TYPE_TIME:
        extract_mysql_time((long) uint3korr(key), &mysql_time);
        break;
      case MYSQL_TYPE_DATETIME:
        extract_mysql_datetime((ulonglong) uint8korr(key), &mysql_time);
        break;
      }

      char timeString[MAX_DATE_STRING_REP_LENGTH];
      my_TIME_to_str(&mysql_time, timeString);
      int length = strlen(timeString);
      key_copy = new uchar[length];
      memcpy(key_copy, timeString, length);
      key_len = length;
    }
    break;
    case MYSQL_TYPE_VARCHAR:
    {
      /**
       * VARCHARs are prefixed with two bytes that represent the actual length of the value.
       * So we need to read the length into actual_length, then copy those bits to key_copy.
       * Thank you, MySQL...
       */
      uint16_t *short_len_ptr = (uint16_t *)key;
      key_len = (uint)(*short_len_ptr);
      key += 2;
      key_copy = new uchar[key_len];
      memcpy(key_copy, key, key_len);
    }
    break;
    default:
      key_copy = new uchar[key_len];
      memcpy(key_copy, key, key_len);
      break;
  }

  jbyteArray java_key = this->env->NewByteArray(key_len);
  this->env->SetByteArrayRegion(java_key, 0, key_len, (jbyte*)key_copy);
  delete[] key_copy;
  jobject index_row = this->env->CallStaticObjectMethod(adapter_class, index_read_method, java_scan_id, java_key, java_find_flag);

  if(read_index_row(index_row, buf) == HA_ERR_END_OF_FILE)
  {
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }

  DBUG_RETURN(0);
}

// Convert an integral type of count bytes to a little endian long
// Convert a buffer of length buff_length into an equivalent long long in long_buff
void CloudHandler::bytes_to_long(const uchar* buff, unsigned int buff_length, const bool is_signed, uchar* long_buff)
{
  if(is_signed && buff[buff_length - 1] >= (uchar) 0x80)
  {
    memset(long_buff, 0xFFFFFFFF, sizeof long_buff);
  } else {
    memset(long_buff, 0x00000000, sizeof long_buff);
  }
  for(int i = 0; i < buff_length; i++)
  {
    long_buff[i] = buff[i];
  }
}

void CloudHandler::store_uuid_ref(jobject index_row, jmethodID get_uuid_method)
{
  jbyteArray uuid = (jbyteArray) this->env->CallObjectMethod(index_row, get_uuid_method);
  uchar* pos = (uchar*) this->env->GetByteArrayElements(uuid, JNI_FALSE);
  memcpy(this->ref, pos, 16);
  this->env->ReleaseByteArrayElements(uuid, (jbyte*)pos, 0);
}

int CloudHandler::index_next(uchar *buf)
{
  int rc = 0;
  my_bitmap_map *orig_bitmap;

  DBUG_ENTER("CloudHandler::index_next");

  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str, TRUE);

  jclass adapter_class = this->adapter();
  jmethodID index_next_method = this->env->GetStaticMethodID(adapter_class, "nextIndexRow", "(J)Lcom/nearinfinity/mysqlengine/jni/IndexRow;");
  jlong java_scan_id = this->curr_scan_id;
  jobject index_row = this->env->CallStaticObjectMethod(adapter_class, index_next_method, java_scan_id);

  if(read_index_row(index_row, buf) == HA_ERR_END_OF_FILE)
  {
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }

  MYSQL_READ_ROW_DONE(rc);

  DBUG_RETURN(rc);
}

jobject CloudHandler::java_find_flag(enum ha_rkey_function find_flag)
{
  const char* index_type_path = "Lcom/nearinfinity/mysqlengine/jni/IndexReadType;";
  jclass read_class = find_jni_class("IndexReadType", this->env);
  jfieldID field_id;
  if (find_flag == HA_READ_KEY_EXACT)
  {
    field_id = this->env->GetStaticFieldID(read_class, "HA_READ_KEY_EXACT", index_type_path);
  }
  else if(find_flag == HA_READ_AFTER_KEY)
  {
    field_id = this->env->GetStaticFieldID(read_class, "HA_READ_AFTER_KEY", index_type_path);
  }
  else if(find_flag == HA_READ_KEY_OR_NEXT)
  {
    field_id = this->env->GetStaticFieldID(read_class, "HA_READ_KEY_OR_NEXT", index_type_path);
  }
  else if(find_flag == HA_READ_KEY_OR_PREV)
  {
    field_id = this->env->GetStaticFieldID(read_class, "HA_READ_KEY_OR_PREV", index_type_path);
  }
  else if(find_flag == HA_READ_BEFORE_KEY)
  {
    field_id = this->env->GetStaticFieldID(read_class, "HA_READ_BEFORE_KEY", index_type_path);
  }
  else
  {
    return NULL;
  }

  return this->env->GetStaticObjectField(read_class, field_id);
}

int CloudHandler::index_prev(uchar *buf)
{
  int rc = 0;
  my_bitmap_map *orig_bitmap;

  DBUG_ENTER("CloudHandler::index_prev");

  orig_bitmap= dbug_tmp_use_all_columns(table, table->write_set);

  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str, TRUE);

  jclass adapter_class = this->adapter();
  jmethodID index_next_method = this->env->GetStaticMethodID(adapter_class, "nextIndexRow", "(J)Lcom/nearinfinity/mysqlengine/jni/IndexRow;");
  jlong java_scan_id = this->curr_scan_id;
  jobject index_row = this->env->CallStaticObjectMethod(adapter_class, index_next_method, java_scan_id);

  if(read_index_row(index_row, buf) == HA_ERR_END_OF_FILE)
  {
    dbug_tmp_restore_column_map(table->write_set, orig_bitmap);
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }

  dbug_tmp_restore_column_map(table->write_set, orig_bitmap);

  MYSQL_READ_ROW_DONE(rc);

  DBUG_RETURN(rc);
}

int CloudHandler::index_first(uchar *buf)
{
  DBUG_ENTER("CloudHandler::index_first");

  jclass adapter_class = this->adapter();
  jmethodID index_read_method = this->env->GetStaticMethodID(adapter_class, "indexRead", "(J[BLcom/nearinfinity/mysqlengine/jni/IndexReadType;)Lcom/nearinfinity/mysqlengine/jni/IndexRow;");
  jlong java_scan_id = this->curr_scan_id;

  jclass read_class = find_jni_class("IndexReadType", this->env);
  jfieldID field_id = this->env->GetStaticFieldID(read_class, "INDEX_FIRST", "Lcom/nearinfinity/mysqlengine/jni/IndexReadType;");
  jobject java_find_flag = this->env->GetStaticObjectField(read_class, field_id);
  jobject index_row = this->env->CallStaticObjectMethod(adapter_class, index_read_method, java_scan_id, NULL, java_find_flag);

  if(read_index_row(index_row, buf) == HA_ERR_END_OF_FILE)
  {
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }

  DBUG_RETURN(0);
}

int CloudHandler::index_last(uchar *buf)
{
  DBUG_ENTER("CloudHandler::index_last");

  jclass adapter_class = this->adapter();
  jmethodID index_read_method = this->env->GetStaticMethodID(adapter_class, "indexRead", "(J[BLcom/nearinfinity/mysqlengine/jni/IndexReadType;)Lcom/nearinfinity/mysqlengine/jni/IndexRow;");
  jlong java_scan_id = this->curr_scan_id;

  jclass read_class = find_jni_class("IndexReadType", this->env);
  jfieldID field_id = this->env->GetStaticFieldID(read_class, "INDEX_LAST", "Lcom/nearinfinity/mysqlengine/jni/IndexReadType;");
  jobject java_find_flag = this->env->GetStaticObjectField(read_class, field_id);
  jobject index_row = this->env->CallStaticObjectMethod(adapter_class, index_read_method, java_scan_id, NULL, java_find_flag);

  if(read_index_row(index_row, buf) == HA_ERR_END_OF_FILE)
  {
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }

  DBUG_RETURN(0);
}

jobject CloudHandler::java_find_flag_by_name(char *name)
{
  jclass read_class = find_jni_class("IndexReadType", this->env);
  jfieldID field_id = this->env->GetStaticFieldID(read_class, name, "Lcom/nearinfinity/mysqlengine/jni/IndexReadType;");
  return this->env->GetStaticObjectField(read_class, field_id);
}

int CloudHandler::read_index_row(jobject index_row, uchar* buf)
{
  jclass index_row_class = find_jni_class("IndexRow", this->env);
  jmethodID get_uuid_method = this->env->GetMethodID(index_row_class, "getUUID", "()[B");
  jmethodID get_rowmap_method = this->env->GetMethodID(index_row_class, "getRowMap", "()Ljava/util/Map;");

  jobject rowMap = this->env->CallObjectMethod(index_row, get_rowmap_method);
  if(rowMap == NULL)
  {
    return HA_ERR_END_OF_FILE;
  }

  this->store_uuid_ref(index_row, get_uuid_method);

  this->java_to_sql(buf, rowMap);

  return 0;
}

bool CloudHandler::is_key_null(const uchar *key)
{
  return key[0] != 0;
}

void CloudHandler::detach_thread()
{
  thread_ref_count--;

  if(thread_ref_count <= 0)
  {
    this->jvm->DetachCurrentThread();
    this->env = NULL;
  }
}

void CloudHandler::attach_thread()
{
  thread_ref_count++;
  JavaVMAttachArgs attachArgs;
  attachArgs.version = JNI_VERSION_1_6;
  attachArgs.name = NULL;
  attachArgs.group = NULL;

  this->jvm->GetEnv((void**)&this->env, attachArgs.version);
  if(this->env == NULL)
  {
    this->jvm->AttachCurrentThread((void**)&this->env, &attachArgs);
  }
}
