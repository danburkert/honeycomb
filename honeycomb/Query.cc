#include "HoneycombHandler.h"
#include "JavaFrame.h"
#include "Java.h"
#include "Logging.h"
#include "Macros.h"
#include "IndexContainer.h"

static IndexContainer::QueryType retrieve_query_flag(enum ha_rkey_function find_flag);

// Index scanning
int HoneycombHandler::index_init(uint idx, bool sorted)
{
  DBUG_ENTER("HoneycombHandler::index_init");
  this->active_index = idx;
  DBUG_RETURN(0);
}

int HoneycombHandler::index_read_map(uchar * buf, const uchar * key,
    key_part_map keypart_map, enum ha_rkey_function find_flag)
{
  DBUG_ENTER("HoneycombHandler::index_read_map");
  IndexContainer index_key;
  uchar* key_ptr = (uchar*)key;

  KEY *key_info = table->s->key_info + this->active_index;
  KEY_PART_INFO *key_part = key_info->key_part;
  KEY_PART_INFO *end_key_part = key_part + key_info->key_parts;

  index_key.set_name(key_info->name);
  index_key.set_type(retrieve_query_flag(find_flag));
  while (key_part < end_key_part && keypart_map)
  {
    uint key_length;
    Field* field = key_part->field;
    bool is_null_field = field->is_null();
    if (is_null_field && key_ptr[0] == 1) // Key is nullable and is actually null
    {
      // Absence is the indicator of null on index key
      key_ptr += key_part->store_length;
      continue;
    }

    // If it is a null field then we have to move past the null byte.
    uchar* key_offset = is_null_field ? key_ptr + 1 : key_ptr;
    uchar* key_copy = create_key_copy(field, key_offset, &key_length, table->in_use);
    index_key.set_record(field->field_name, (char*)key_copy, key_length);
    ARRAY_DELETE(key_copy);
    key_ptr += key_part->store_length;
    key_part++;
  }

  DBUG_RETURN(start_index_scan(index_key, buf));
}

int HoneycombHandler::index_first(uchar *buf)
{
  DBUG_ENTER("HoneycombHandler::index_first");
  int rc = full_index_scan(buf, IndexContainer::INDEX_FIRST);
  DBUG_RETURN(rc);
}

int HoneycombHandler::index_last(uchar *buf)
{
  DBUG_ENTER("HoneycombHandler::index_last");
  int rc = full_index_scan(buf, IndexContainer::INDEX_LAST);
  DBUG_RETURN(rc);
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

int HoneycombHandler::index_end()
{
  DBUG_ENTER("HoneycombHandler::index_end");

  this->env->CallVoidMethod(handler_proxy, cache->handler_proxy().end_scan);

  DBUG_RETURN(0);
}

// Full table scanning
int HoneycombHandler::rnd_pos(uchar *buf, uchar *pos)
{
  int rc = 0;
  ha_statistic_increment(&SSV::ha_read_rnd_count); // Boilerplate
  DBUG_ENTER("HoneycombHandler::rnd_pos");

  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str, FALSE);

  JavaFrame frame(env, 3);

  jbyteArray uuid = convert_value_to_java_bytes(pos, 16, this->env);
  jbyteArray row_jbuf = static_cast<jbyteArray>(this->env->CallObjectMethod(
        handler_proxy, cache->handler_proxy().get_row, uuid));
  rc = check_exceptions(env, cache, "HoneycombHandler::rnd_pos");
  if (rc != 0)
    DBUG_RETURN(rc);

  rc = read_bytes_into_mysql(row_jbuf, buf);

  MYSQL_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}

int HoneycombHandler::rnd_init(bool scan)
{
  DBUG_ENTER("HoneycombHandler::rnd_init");

  this->env->CallVoidMethod(handler_proxy, cache->handler_proxy().start_table_scan);
  int rc = check_exceptions(env, cache, "HoneycombHandler::rnd_init");
  if (rc != 0)
    return rc;

  this->performing_scan = scan;

  DBUG_RETURN(rc);
}

int HoneycombHandler::rnd_next(uchar *buf)
{
  int rc = 0;

  ha_statistic_increment(&SSV::ha_read_rnd_next_count);
  DBUG_ENTER("HoneycombHandler::rnd_next");
  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str, TRUE);

  rc = get_next_row(buf);

  MYSQL_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}

void HoneycombHandler::position(const uchar *record)
{
  DBUG_ENTER("HoneycombHandler::position");
  DBUG_VOID_RETURN;
}

int HoneycombHandler::rnd_end()
{
  DBUG_ENTER("HoneycombHandler::rnd_end");

  this->env->CallVoidMethod(handler_proxy, cache->handler_proxy().end_scan);

  DBUG_RETURN(0);
}

// Scan helpers

int HoneycombHandler::full_index_scan(uchar* buf, IndexContainer::QueryType query)
{
  IndexContainer index_key;
  KEY *key_info = table->s->key_info + this->active_index;
  index_key.set_type(query);
  index_key.set_name(key_info->name);
  return start_index_scan(index_key, buf);
}

int HoneycombHandler::start_index_scan(Serializable& index_key, uchar* buf)
{
  jbyteArray jserialized_key = serialize_to_java(index_key);
  this->env->CallVoidMethod(handler_proxy, cache->handler_proxy().start_index_scan, jserialized_key);
  int rc = check_exceptions(env, cache, "HoneycombHandler::start_index_scan");
  if (rc != 0)
    return rc;
  return get_next_row(buf);
}

int HoneycombHandler::retrieve_value_from_index(uchar* buf)
{
  int rc = 0;

  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str, TRUE);
  my_bitmap_map * orig_bitmap = dbug_tmp_use_all_columns(table, table->read_set);

  rc = get_next_row(buf);

  dbug_tmp_restore_column_map(table->read_set, orig_bitmap);
  MYSQL_READ_ROW_DONE(rc);

  return rc;
}

int HoneycombHandler::get_next_row(uchar* buf)
{
  JavaFrame frame(env, 1);
  jbyteArray row_bytes = static_cast<jbyteArray>(this->env->CallObjectMethod(handler_proxy, 
        cache->handler_proxy().get_next_row));
  int rc = check_exceptions(env, cache, "HoneycombHandler::get_next_row");
  if (rc != 0)
    return rc;

  return read_bytes_into_mysql(row_bytes, buf);
}

int HoneycombHandler::read_bytes_into_mysql(jbyteArray row_bytes, uchar* buf)
{
  if (row_bytes == NULL)
  {
    this->table->status = STATUS_NOT_FOUND;
    return HA_ERR_END_OF_FILE;
  }

  deserialized_from_java(row_bytes, *this->row);
  return read_row(buf);
}

int HoneycombHandler::read_row(uchar *buf)
{
  store_uuid_ref(this->row);

  if (java_to_sql(buf, this->row))
  {
    this->table->status = STATUS_NOT_READ;
    return HA_ERR_INTERNAL_ERROR;
  }

  this->table->status = 0;
  return 0;
}

static IndexContainer::QueryType retrieve_query_flag(enum ha_rkey_function find_flag)
{
  switch(find_flag)
  {
    case HA_READ_KEY_EXACT:
      return IndexContainer::EXACT_KEY;
    case HA_READ_KEY_OR_NEXT:
      return IndexContainer::KEY_OR_NEXT;
    case HA_READ_KEY_OR_PREV:            
    case HA_READ_PREFIX_LAST_OR_PREV:
      return IndexContainer::KEY_OR_PREVIOUS;
    case HA_READ_AFTER_KEY:             
      return IndexContainer::AFTER_KEY;
    case HA_READ_BEFORE_KEY:           
      return IndexContainer::BEFORE_KEY;
    default:
      return (IndexContainer::QueryType)-1;
  }
}
