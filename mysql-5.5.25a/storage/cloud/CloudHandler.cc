#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation        // gcc: Class implementation
#endif

#include "sql_priv.h"
#include "sql_class.h"           // MYSQL_HANDLERTON_INTERFACE_VERSION
#include "CloudHandler.h"
#include "probes_mysql.h"
#include "sql_plugin.h"
#include "ha_cloud.h"
#include "JVMThreadAttach.h"
#include "mysql_time.h"

#include <sys/time.h>
/*
  If frm_error() is called in table.cc this is called to find out what file
  extensions exist for this handler.

  // TODO: Do any extensions exist for this handler? Doesn't seem like it. - ABC
*/
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

double timing(clock_t begin, clock_t end)
{
  return (double)((end - begin)*1000) / CLOCKS_PER_SEC;
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

  int ret = write_row_helper();

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
  write_row_helper();

  DBUG_RETURN(0);
}

int CloudHandler::delete_row(const uchar *buf)
{
  DBUG_ENTER("CloudHandler::delete_row");
  ha_statistic_increment(&SSV::ha_delete_count);
  //stats.records--;
  //share->rows_recorded--;
  delete_row_helper();
  DBUG_RETURN(0);
}

int CloudHandler::delete_row_helper()
{
  DBUG_ENTER("CloudHandler::delete_row_helper");


  jclass adapter_class = this->env->FindClass("com/nearinfinity/mysqlengine/jni/HBaseAdapter");
  jmethodID delete_row_method = this->env->GetStaticMethodID(adapter_class, "deleteRow", "(J)Z");
  jlong java_scan_id = curr_scan_id;

  jboolean result = this->env->CallStaticBooleanMethod(adapter_class, delete_row_method, java_scan_id);

  DBUG_RETURN(0);
}

int CloudHandler::rnd_init(bool scan)
{
  DBUG_ENTER("CloudHandler::rnd_init");

  const char* table_name = this->table->alias;

  JavaVMAttachArgs attachArgs;
  attachArgs.version = JNI_VERSION_1_6;
  attachArgs.name = NULL;
  attachArgs.group = NULL;
  this->jvm->AttachCurrentThread((void**)&this->env, &attachArgs);

  jclass adapter_class = this->env->FindClass("com/nearinfinity/mysqlengine/jni/HBaseAdapter");
  jmethodID start_scan_method = this->env->GetStaticMethodID(adapter_class, "startScan", "(Ljava/lang/String;Z)J");
  jstring java_table_name = this->string_to_java_string(table_name);

  jboolean java_scan_boolean = scan ? JNI_TRUE : JNI_FALSE;

  this->curr_scan_id = this->env->CallStaticLongMethod(adapter_class, start_scan_method, java_table_name, java_scan_boolean);

  this->performing_scan = scan;

  DBUG_RETURN(0);
}

int CloudHandler::rnd_next(uchar *buf)
{
  int rc = 0;
  my_bitmap_map *orig_bitmap;

  ha_statistic_increment(&SSV::ha_read_rnd_next_count);
  DBUG_ENTER("CloudHandler::rnd_next");

  orig_bitmap= dbug_tmp_use_all_columns(table, table->write_set);

  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str, TRUE);

  memset(buf, 0, table->s->null_bytes);

  jclass adapter_class = this->env->FindClass("com/nearinfinity/mysqlengine/jni/HBaseAdapter");
  jmethodID next_row_method = this->env->GetStaticMethodID(adapter_class, "nextRow", "(J)Lcom/nearinfinity/mysqlengine/jni/Row;");
  jlong java_scan_id = curr_scan_id;
  jobject row = this->env->CallStaticObjectMethod(adapter_class, next_row_method, java_scan_id);

  jclass row_class = this->env->FindClass("com/nearinfinity/mysqlengine/jni/Row");
  jmethodID get_keys_method = this->env->GetMethodID(row_class, "getKeys", "()[Ljava/lang/String;");
  jmethodID get_vals_method = this->env->GetMethodID(row_class, "getValues", "()[[B");
  jmethodID get_row_map_method = this->env->GetMethodID(row_class, "getRowMap", "()Ljava/util/Map;");
  jmethodID get_uuid_method = this->env->GetMethodID(row_class, "getUUID", "()[B");

  jobject row_map = this->env->CallObjectMethod(row, get_row_map_method);
  jarray keys = (jarray) this->env->CallObjectMethod(row, get_keys_method);
  jarray vals = (jarray) this->env->CallObjectMethod(row, get_vals_method);
  jbyteArray uuid = (jbyteArray) this->env->CallObjectMethod(row, get_uuid_method);

  if (java_map_is_empty(row_map))
  {
    dbug_tmp_restore_column_map(table->write_set, orig_bitmap);
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }

  this->ref = (uchar*) this->env->GetByteArrayElements(uuid, JNI_FALSE);
  this->ref_length = 16;

  java_to_sql(buf, row_map);

  dbug_tmp_restore_column_map(table->write_set, orig_bitmap);

  stats.records++;
  MYSQL_READ_ROW_DONE(rc);

  DBUG_RETURN(rc);
}

void CloudHandler::java_to_sql(uchar* buf, jobject row_map)
{
  jboolean is_copy = JNI_FALSE;

  for (int i = 0; i < table->s->fields; i++)
  {
    Field *field = table->field[i];
    const char* key = field->field_name;
    jstring java_key = string_to_java_string(key);
    jbyteArray java_val = java_map_get(row_map, java_key);
    char* val = (char*) this->env->GetByteArrayElements(java_val, &is_copy);
    jsize val_length = this->env->GetArrayLength(java_val);

    my_ptrdiff_t offset = (my_ptrdiff_t) (buf - this->table->record[0]);
    enum_field_types field_type = field->type();
    field->move_field_offset(offset);

    if (field_type == MYSQL_TYPE_LONG ||
        field_type == MYSQL_TYPE_SHORT ||
        field_type == MYSQL_TYPE_LONGLONG ||
        field_type == MYSQL_TYPE_INT24 ||
        field_type == MYSQL_TYPE_TINY ||
        field_type == MYSQL_TYPE_YEAR)
    {
      longlong long_value = *(longlong*)val;
      if(this->is_little_endian())
      {
        long_value = __builtin_bswap64(long_value);
      }

      field->store(long_value, false);
    }
    else if (field_type == MYSQL_TYPE_FLOAT ||
             field_type == MYSQL_TYPE_DECIMAL ||
             field_type == MYSQL_TYPE_NEWDECIMAL ||
             field_type == MYSQL_TYPE_DOUBLE)
    {
      double double_value;
      if (this->is_little_endian())
      {
        longlong* long_ptr = (longlong*)val;
        longlong swapped_long = __builtin_bswap64(*long_ptr);
        double_value = *(double*)&swapped_long;
      }
      else
      {
        double_value = *(double*)val;
      }

      field->store(double_value);
    }
    else if (field_type == MYSQL_TYPE_VARCHAR
        || field_type == MYSQL_TYPE_STRING
        || field_type == MYSQL_TYPE_VAR_STRING
        || field_type == MYSQL_TYPE_BLOB
        || field_type == MYSQL_TYPE_TINY_BLOB
        || field_type == MYSQL_TYPE_MEDIUM_BLOB
        || field_type == MYSQL_TYPE_LONG_BLOB
        || field_type == MYSQL_TYPE_ENUM)
    {
      field->store(val, val_length, &my_charset_bin);
    }
    else if (field_type == MYSQL_TYPE_TIME
        || field_type == MYSQL_TYPE_DATE
        || field_type == MYSQL_TYPE_DATETIME
        || field_type == MYSQL_TYPE_TIMESTAMP
        || field_type == MYSQL_TYPE_NEWDATE)
    {
      MYSQL_TIME mysql_time;

      int was_cut;
      int warning;

      switch (field_type)
      {
      case MYSQL_TYPE_TIME:
        str_to_time(val, field->field_length, &mysql_time, &warning);
        break;
      default:
        str_to_datetime(val, field->field_length, &mysql_time, TIME_FUZZY_DATE, &was_cut);
        break;
      }

      field->store_time(&mysql_time, mysql_time.time_type);
    }

    field->move_field_offset(-offset);
    this->env->ReleaseByteArrayElements(java_val, (jbyte*)val, 0);
  }
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
  DBUG_ENTER("CloudHandler::rnd_pos");
  ha_statistic_increment(&SSV::ha_read_rnd_count); // Boilerplate
  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str, FALSE);

  jclass adapter_class = this->env->FindClass("com/nearinfinity/mysqlengine/jni/HBaseAdapter");
  jmethodID get_row_method = this->env->GetStaticMethodID(adapter_class, "getRow", "(JLjava/lang/String;[B)Lcom/nearinfinity/mysqlengine/jni/Row;");
  jlong java_scan_id = curr_scan_id;
  jobject row = this->env->CallStaticObjectMethod(adapter_class, get_row_method, java_scan_id, pos);

  jclass row_class = this->env->FindClass("com/nearinfinity/mysqlengine/jni/Row");
  jmethodID get_keys_method = this->env->GetMethodID(row_class, "getKeys", "()[Ljava/lang/String;");
  jmethodID get_vals_method = this->env->GetMethodID(row_class, "getValues", "()[[B");

  jarray keys = (jarray) this->env->CallObjectMethod(row, get_keys_method);
  jarray vals = (jarray) this->env->CallObjectMethod(row, get_vals_method);

  jboolean is_copy = JNI_FALSE;

  if (this->env->GetArrayLength(keys) == 0 ||
      this->env->GetArrayLength(vals) == 0)
  {
    DBUG_RETURN(HA_ERR_WRONG_COMMAND);
  }

  //store_field_values(buf, keys, vals);

  MYSQL_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}

int CloudHandler::rnd_end()
{
  DBUG_ENTER("CloudHandler::rnd_end");

  jclass adapter_class = this->env->FindClass("com/nearinfinity/mysqlengine/jni/HBaseAdapter");
  jmethodID end_scan_method = this->env->GetStaticMethodID(adapter_class, "endScan", "(J)V");
  jlong java_scan_id = curr_scan_id;

  this->env->CallStaticVoidMethod(adapter_class, end_scan_method, java_scan_id);
  this->jvm->DetachCurrentThread();

  curr_scan_id = -1;
  this->performing_scan = false;
  DBUG_RETURN(0);
}

void CloudHandler::start_bulk_insert(ha_rows rows)
{
  DBUG_ENTER("CloudHandler::start_bulk_insert");
  JavaVMAttachArgs attachArgs;
  attachArgs.version = JNI_VERSION_1_6;
  attachArgs.name = NULL;
  attachArgs.group = NULL;
  this->jvm->AttachCurrentThread((void**)&this->env, &attachArgs);

  DBUG_VOID_RETURN;
}

int CloudHandler::end_bulk_insert()
{
  DBUG_ENTER("CloudHandler::end_bulk_insert");
  this->jvm->DetachCurrentThread();
  DBUG_RETURN(0);
}

int CloudHandler::create(const char *name, TABLE *table_arg,
                         HA_CREATE_INFO *create_info)
{
  DBUG_ENTER("CloudHandler::create");

  JVMThreadAttach attached_thread(&this->env, this->jvm);
  jclass adapter_class = this->env->FindClass("com/nearinfinity/mysqlengine/jni/HBaseAdapter");
  if (adapter_class == NULL)
  {
    this->print_java_exception(this->env);
    ERROR(("Could not find adapter class HBaseAdapter"));
    DBUG_RETURN(1);
  }

  const char* table_name = create_info->alias;

  jclass list_class = this->env->FindClass("java/util/LinkedList");
  jmethodID list_constructor = this->env->GetMethodID(list_class, "<init>", "()V");
  jobject columns = this->env->NewObject(list_class, list_constructor);
  jmethodID add_column = this->env->GetMethodID(list_class, "add", "(Ljava/lang/Object;)Z");

  for (Field **field = table_arg->field ; *field ; field++)
  {
    this->env->CallBooleanMethod(columns, add_column, string_to_java_string((*field)->field_name));
  }

  jmethodID create_table_method = this->env->GetStaticMethodID(adapter_class, "createTable", "(Ljava/lang/String;Ljava/util/List;)Z");
  jboolean result = this->env->CallStaticBooleanMethod(adapter_class, create_table_method, string_to_java_string(table_name), columns);
  INFO(("Result of createTable: %d", result));
  this->print_java_exception(this->env);

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
    //mysql_mutex_destroy(&share->mutex);
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
  char *tmp_alias;
  uint path_length, alias_length;

  rec_buffer= create_record_buffer(table->s->reclength);

  if (!rec_buffer)
  {
    DBUG_PRINT("CloudHandler", ("Ran out of memory while allocating record buffer"));

    return NULL;
  }

  mysql_mutex_lock(cloud_mutex);
  path_length=(uint) strlen(table_name);
  alias_length=(uint) strlen(table->alias);

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
                         &tmp_alias, alias_length+1,
                         NullS))
    {
      mysql_mutex_unlock(cloud_mutex);
      return NULL;
    }
  }

  share->use_count= 0;
  share->table_path_length= path_length;
  share->path_to_table= tmp_path_name;
  share->table_alias= tmp_alias;
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

/* Set up the JNI Environment, and then persist the row to HBase.
 * This helper calls sql_to_java, which returns the row information
 * as a jobject to be sent to the HBaseAdapter.
 */
int CloudHandler::write_row_helper() {
  DBUG_ENTER("CloudHandler::write_row_helper");

  jclass adapter_class = this->env->FindClass("com/nearinfinity/mysqlengine/jni/HBaseAdapter");
  jmethodID write_row_method = this->env->GetStaticMethodID(adapter_class, "writeRow", "(Ljava/lang/String;Ljava/util/Map;)Z");
  jstring java_table_name = this->string_to_java_string(this->share->table_alias);
  jobject java_row_map = sql_to_java();

  this->env->CallStaticBooleanMethod(adapter_class, write_row_method, java_table_name, java_row_map);

  DBUG_RETURN(0);
}

/* Read fields into a java map.
 */
jobject CloudHandler::sql_to_java() {
  jobject java_map = this->create_java_map();
  // Boilerplate stuff every engine has to do on writes

  if (table->timestamp_field_type & TIMESTAMP_AUTO_SET_ON_INSERT)
    table->timestamp_field->set_time();

  my_bitmap_map *old_map = dbug_tmp_use_all_columns(table, table->read_set);

  char attribute_buffer[1024];
  String attribute(attribute_buffer, sizeof(attribute_buffer), &my_charset_bin);

  for (Field **field_ptr=table->field; *field_ptr; field_ptr++)
  {
    Field * field = *field_ptr;

    memset(rec_buffer->buffer, 0, rec_buffer->length);

    const bool was_null= field->is_null();

    if (was_null)
    {
      field->set_default();
      field->set_notnull();
    }

    int fieldType = field->type();
    uint actualFieldSize = field->field_length;

    if (fieldType == MYSQL_TYPE_LONG
        || fieldType == MYSQL_TYPE_SHORT
        || fieldType == MYSQL_TYPE_TINY
        || fieldType == MYSQL_TYPE_LONGLONG
        || fieldType == MYSQL_TYPE_INT24
        || fieldType == MYSQL_TYPE_ENUM
        || fieldType == MYSQL_TYPE_YEAR)
    {
      longlong field_value = field->val_int();
      if(this->is_little_endian())
      {
        field_value = __builtin_bswap64(field_value);
      }

      actualFieldSize = sizeof(longlong);
      memcpy(rec_buffer->buffer, &field_value, sizeof(longlong));
    }
    else if (fieldType == MYSQL_TYPE_DOUBLE
             || fieldType == MYSQL_TYPE_FLOAT
             || fieldType == MYSQL_TYPE_DECIMAL
             || fieldType == MYSQL_TYPE_NEWDECIMAL)
    {
      double field_value = field->val_real();
      actualFieldSize = sizeof(double);
      if(this->is_little_endian())
      {
        longlong* long_value = (longlong*)&field_value;
        *long_value = __builtin_bswap64(*long_value);
      }
      memcpy(rec_buffer->buffer, &field_value, sizeof(longlong));
    }
	else if (fieldType == MYSQL_TYPE_TIME
			|| fieldType == MYSQL_TYPE_DATE
			|| fieldType == MYSQL_TYPE_NEWDATE
			|| fieldType == MYSQL_TYPE_DATETIME
			|| fieldType == MYSQL_TYPE_TIMESTAMP)
	{
		MYSQL_TIME mysql_time;
		field->get_time(&mysql_time);

		switch (fieldType)
		{
			case MYSQL_TYPE_DATE:
			case MYSQL_TYPE_NEWDATE:
				mysql_time.time_type = MYSQL_TIMESTAMP_DATE;
				break;
			case MYSQL_TYPE_DATETIME:
			case MYSQL_TYPE_TIMESTAMP:
				mysql_time.time_type = MYSQL_TIMESTAMP_DATETIME;
				break;
			case MYSQL_TYPE_TIME:
				mysql_time.time_type = MYSQL_TIMESTAMP_TIME;
				break;
			default:
				mysql_time.time_type = MYSQL_TIMESTAMP_NONE;
				break;
		}

		char timeString[MAX_DATE_STRING_REP_LENGTH];
		my_TIME_to_str(&mysql_time, timeString);

		actualFieldSize = strlen(timeString);
		memcpy(rec_buffer->buffer, timeString, actualFieldSize);
	}
    else if (fieldType == MYSQL_TYPE_VARCHAR
             || fieldType == MYSQL_TYPE_STRING
             || fieldType == MYSQL_TYPE_VAR_STRING
             || fieldType == MYSQL_TYPE_BLOB
             || fieldType == MYSQL_TYPE_TINY_BLOB
             || fieldType == MYSQL_TYPE_MEDIUM_BLOB
             || fieldType == MYSQL_TYPE_LONG_BLOB)
    {
      field->val_str(&attribute);
      actualFieldSize = attribute.length();
      memcpy(rec_buffer->buffer, attribute.ptr(), attribute.length());
    }
	else
	{
		memcpy(rec_buffer->buffer, field->ptr, field->field_length);
	}

    if (was_null)
    {
      field->set_null();
    }

    jstring field_name = this->string_to_java_string(field->field_name);
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

jstring CloudHandler::string_to_java_string(const char* string)
{
  return this->env->NewStringUTF(string);
}

jobject CloudHandler::create_java_map()
{
  jclass map_class = this->env->FindClass("java/util/TreeMap");
  jmethodID constructor = this->env->GetMethodID(map_class, "<init>", "()V");
  jobject java_map = this->env->NewObject(map_class, constructor);
  return java_map;
}

jobject CloudHandler::java_map_insert(jobject java_map, jstring key, jbyteArray value)
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
int CloudHandler::add_index(TABLE *table_arg, KEY *key_info, uint num_of_keys, handler_add_index **add)
{
  DBUG_ENTER("CloudHandler::add_index");
  
  *add = new handler_add_index(table_arg, key_info, num_of_keys);

  DBUG_RETURN(0);
}

int CloudHandler::final_add_index(handler_add_index *add, bool commit)
{
  DBUG_ENTER("CloudHandler::final_add_index");

  DBUG_RETURN(0);
}

int CloudHandler::prepare_drop_index(TABLE *table_arg, uint *key_num, uint num_of_keys)
{
  DBUG_ENTER("CloudHandler::prepare_drop_index");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

int CloudHandler::final_drop_index(TABLE *table_arg)
{
  DBUG_ENTER("CloudHandler::final_drop_index");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

int CloudHandler::index_init(uint idx, bool sorted)
{
  DBUG_ENTER("CloudHandler::index_init");
  
  this->active_index = idx;
  
  const char* table_name = this->table->alias;

  JavaVMAttachArgs attachArgs;
  attachArgs.version = JNI_VERSION_1_6;
  attachArgs.name = NULL;
  attachArgs.group = NULL;
  this->jvm->AttachCurrentThread((void**)&this->env, &attachArgs);

  jclass adapter_class = this->env->FindClass("com/nearinfinity/mysqlengine/jni/HBaseAdapter");
  jmethodID start_scan_method = this->env->GetStaticMethodID(adapter_class, "startIndexScan", "(Ljava/lang/String;Ljava/lang/String;)J");
  jstring java_table_name = this->string_to_java_string(table_name);

  this->curr_scan_id = this->env->CallStaticLongMethod(adapter_class, start_scan_method, java_table_name);
  
  DBUG_RETURN(0);
}

int CloudHandler::index_end()
{
  DBUG_ENTER("CloudHandler::index_end");
  
  jclass adapter_class = this->env->FindClass("com/nearinfinity/mysqlengine/jni/HBaseAdapter");
  jmethodID end_scan_method = this->env->GetStaticMethodID(adapter_class, "endScan", "(J)V");
  jlong java_scan_id = curr_scan_id;

  this->env->CallStaticVoidMethod(adapter_class, end_scan_method, java_scan_id);
  //INFO(("Total HBase time %f ms", this->share->hbase_time));
  //this->share->hbase_time = 0;
  this->jvm->DetachCurrentThread();

  this->curr_scan_id = -1;
  this->active_index = -1;

  DBUG_RETURN(0);
}

int CloudHandler::index_read(uchar *buf, const uchar *key, uint key_len, enum ha_rkey_function find_flag)
{
  DBUG_ENTER("CloudHandler::index_read");
  
  jclass adapter_class = this->env->FindClass("com/nearinfinity/mysqlengine/jni/HBaseAdapter");
  jmethodID get_row_by_value_method = this->env->GetStaticMethodID(adapter_class, "getRowByValue", "(J[B)Lcom/nearinfinity/mysqlengine/jni/Row;");

   

  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

int CloudHandler::index_next(uchar *buf)
{
  int rc = 0;
  my_bitmap_map *orig_bitmap;

  //ha_statistic_increment(&SSV::ha_read_rnd_next_count);
  DBUG_ENTER("CloudHandler::index_next");

  orig_bitmap= dbug_tmp_use_all_columns(table, table->write_set);

  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str, TRUE);

  memset(buf, 0, table->s->null_bytes);

  jclass adapter_class = this->env->FindClass("com/nearinfinity/mysqlengine/jni/HBaseAdapter");
  jmethodID next_row_method = this->env->GetStaticMethodID(adapter_class, "nextIndexRow", "(J)Lcom/nearinfinity/mysqlengine/jni/Row;");
  jlong java_scan_id = curr_scan_id;
  jobject row = this->env->CallStaticObjectMethod(adapter_class, next_row_method, java_scan_id);

  jclass row_class = this->env->FindClass("com/nearinfinity/mysqlengine/jni/Row");
  jmethodID get_keys_method = this->env->GetMethodID(row_class, "getKeys", "()[Ljava/lang/String;");
  jmethodID get_vals_method = this->env->GetMethodID(row_class, "getValues", "()[[B");
  jmethodID get_row_map_method = this->env->GetMethodID(row_class, "getRowMap", "()Ljava/util/Map;");
  jmethodID get_uuid_method = this->env->GetMethodID(row_class, "getUUID", "()[B");

  jobject row_map = this->env->CallObjectMethod(row, get_row_map_method);
  jarray keys = (jarray) this->env->CallObjectMethod(row, get_keys_method);
  jarray vals = (jarray) this->env->CallObjectMethod(row, get_vals_method);
  jbyteArray uuid = (jbyteArray) this->env->CallObjectMethod(row, get_uuid_method);

  if (java_map_is_empty(row_map))
  {
    dbug_tmp_restore_column_map(table->write_set, orig_bitmap);
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }

  //this->ref = (uchar*) this->env->GetByteArrayElements(uuid, JNI_FALSE);
  //this->ref_length = 16;

  java_to_sql(buf, row_map);

  dbug_tmp_restore_column_map(table->write_set, orig_bitmap);

  //stats.records++;
  MYSQL_READ_ROW_DONE(rc);

  DBUG_RETURN(rc);
}

int CloudHandler::index_prev(uchar *buf)
{
  DBUG_ENTER("CloudHandler::index_prev");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

int CloudHandler::index_first(uchar *buf)
{
  DBUG_ENTER("CloudHandler::index_first");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

int CloudHandler::index_last(uchar *buf)
{
  DBUG_ENTER("CloudHandler::index_last");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}
