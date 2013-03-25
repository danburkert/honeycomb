#include "HoneycombHandler.h"
#include "JavaFrame.h"
#include "Java.h"
#include "Logging.h"
#include "Macros.h"

int HoneycombHandler::index_end()
{
  DBUG_ENTER("HoneycombHandler::index_end");

  this->end_scan();

  DBUG_RETURN(0);
}

int HoneycombHandler::get_index_row(jfieldID field_id, uchar* buf)
{
  JavaFrame frame(env, 2);
  jclass adapter_class = cache->hbase_adapter().clazz;
  jmethodID index_read_method = cache->hbase_adapter().index_read;
  jclass read_class = cache->index_read_type().clazz;
  jobject java_find_flag = this->env->GetStaticObjectField(read_class, field_id);
  jbyteArray row_jbuf = (jbyteArray) this->env->CallStaticObjectMethod(adapter_class,
      index_read_method, this->curr_scan_id, NULL, java_find_flag);
  EXCEPTION_CHECK_IE("HoneycombHandler::get_index_row", "calling getIndexRow");

  if (row_jbuf == NULL)
  {
    this->table->status = STATUS_NOT_FOUND;
    return HA_ERR_END_OF_FILE;
  }

  jbyte* row_buf = this->env->GetByteArrayElements(row_jbuf, JNI_FALSE);
  NULL_CHECK_ABORT(row_buf, "HoneycombHandler::get_index_row: OutOfMemoryError while calling GetByteArrayElements");
  this->row->deserialize((const char*) row_buf, this->env->GetArrayLength(row_jbuf));
  this->env->ReleaseByteArrayElements(row_jbuf, row_buf, 0);

  int rc = read_row(buf, this->row);
  return rc;
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

  JavaFrame frame(env, 3);
  jclass adapter_class = cache->hbase_adapter().clazz;
  jmethodID get_row_method = cache->hbase_adapter().get_row;
  jbyteArray uuid = convert_value_to_java_bytes(pos, 16, this->env);

  jbyteArray row_jbuf = (jbyteArray) this->env->CallStaticObjectMethod(adapter_class,
      get_row_method, this->curr_scan_id, uuid);
  EXCEPTION_CHECK_DBUG_IE("HoneycombHandler::rnd_pos", "calling getRow");

  if (row_jbuf == NULL)
  {
    this->table->status = STATUS_NOT_FOUND;
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }

  jbyte* row_buf = this->env->GetByteArrayElements(row_jbuf, JNI_FALSE);
  NULL_CHECK_ABORT(row_buf, "HoneycombHandler::rnd_next: OutOfMemoryError while calling GetByteArrayElements");
  this->row->deserialize((const char*) row_buf, this->env->GetArrayLength(row_jbuf));
  this->env->ReleaseByteArrayElements(row_jbuf, row_buf, 0);

  if (java_to_sql(buf, this->row))
  {
    this->table->status = STATUS_NOT_READ;
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }
  store_uuid_ref(this->row);
  this->table->status = 0;

  MYSQL_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}

int HoneycombHandler::rnd_end()
{
  DBUG_ENTER("HoneycombHandler::rnd_end");

  this->end_scan();

  DBUG_RETURN(0);
}

int HoneycombHandler::rnd_init(bool scan)
{
  DBUG_ENTER("HoneycombHandler::rnd_init");

  JavaFrame frame(env, 1);
  jclass adapter_class = cache->hbase_adapter().clazz;
  jmethodID start_scan_method = cache->hbase_adapter().start_scan;
  jstring table_name = this->table_name();

  this->terminate_scan();
  this->curr_scan_id = this->env->CallStaticLongMethod(adapter_class,
      start_scan_method, table_name, scan);
  EXCEPTION_CHECK_DBUG_IE("HoneycombHandler::rnd_init", "calling startScan");

  this->performing_scan = scan;

  DBUG_RETURN(0);
}

int HoneycombHandler::rnd_next(uchar *buf)
{
  int rc = 0;

  ha_statistic_increment(&SSV::ha_read_rnd_next_count);
  DBUG_ENTER("HoneycombHandler::rnd_next");

  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str, TRUE);
  JavaFrame frame(env, 2);

  memset(buf, 0, table->s->null_bytes);
  jclass adapter_class = cache->hbase_adapter().clazz;
  jmethodID next_row_method = cache->hbase_adapter().next_row;

  jbyteArray row_jbuf = (jbyteArray) this->env->CallStaticObjectMethod(adapter_class,
      next_row_method, this->curr_scan_id);
  EXCEPTION_CHECK_DBUG_IE("HoneycombHandler::rnd_next", "calling nextRow");
  if (row_jbuf == NULL)
  {
    this->table->status = STATUS_NOT_FOUND;
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }

  jbyte* row_buf = this->env->GetByteArrayElements(row_jbuf, JNI_FALSE);
  NULL_CHECK_ABORT(row_buf, "HoneycombHandler::rnd_next: OutOfMemoryError while calling GetByteArrayElements");
  this->row->deserialize((const char*) row_buf, this->env->GetArrayLength(row_jbuf));
  this->env->ReleaseByteArrayElements(row_jbuf, row_buf, 0);

  store_uuid_ref(this->row);

  if (java_to_sql(buf, this->row))
  {
    this->table->status = STATUS_NOT_READ;
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }
  this->table->status = 0;

  MYSQL_READ_ROW_DONE(rc);

  EXCEPTION_CHECK("HoneycombHandler::rnd_next", "leaving function.");
  DBUG_RETURN(rc);
}

void HoneycombHandler::terminate_scan()
{
  if (this->curr_scan_id != -1)
  {
    JavaFrame frame(env, 1);
    jclass adapter_class = cache->hbase_adapter().clazz;
    jmethodID end_scan_method = cache->hbase_adapter().end_scan;
    this->env->CallStaticVoidMethod(adapter_class, end_scan_method, this->curr_scan_id);
    EXCEPTION_CHECK("HoneycombHandler::terminate_scan", "leaving function.");
    this->curr_scan_id = -1;
  }
}

void HoneycombHandler::end_scan()
{
}
