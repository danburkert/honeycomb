#include "HoneycombHandler.h"
#include "JavaFrame.h"
#include "Java.h"
#include "Logging.h"
#include "Macros.h"
#include "IndexContainer.h"

static IndexContainer::QueryType retrieve_query_flag(enum ha_rkey_function find_flag);

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
  int rc;
  JavaFrame frame(env, 1);
  uint key_length, index = 0;
  uchar* key_ptr = (uchar*)key;
  uchar* key_copy;
  IndexContainer index_key;

  KEY *key_info = table->s->key_info + this->active_index;
  KEY_PART_INFO *key_part = key_info->key_part;
  KEY_PART_INFO *end_key_part = key_part + key_info->key_parts;

  index_key.set_name(key_info->name);
  index_key.set_type(retrieve_query_flag(find_flag));
  while (key_part < end_key_part && keypart_map)
  {
    Field* field = key_part->field;
    if (this->is_field_nullable(this->table_name(), field->field_name)
        && key_ptr[0] == 1) // Key is null
    {
      key_ptr += key_part->store_length;
      continue;
    }

    key_copy = create_key_copy(field, key_ptr, &key_length, table->in_use);
    index_key.set_bytes_record(field->field_name, (char*)key_copy, key_length);
    ARRAY_DELETE(key_copy);
    key_ptr += key_part->store_length;
    index++;
    key_part++;
  }

  jbyteArray jserialized_key = serialize_to_java(index_key);
  this->env->CallVoidMethod(handler_proxy, cache->handler_proxy().start_index_scan, jserialized_key);
  rc = get_next_index_row(buf);

  DBUG_RETURN(rc);
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

int HoneycombHandler::full_index_scan(uchar* buf, IndexContainer::QueryType query)
{
  IndexContainer index_key;
  KEY *key_info = table->s->key_info + this->active_index;
  index_key.set_type(query);
  index_key.set_name(key_info->name);
  jbyteArray jserialized_key = serialize_to_java(index_key);
  this->env->CallVoidMethod(handler_proxy, cache->handler_proxy().start_index_scan, jserialized_key);
  return get_next_index_row(buf);
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

  this->env->CallVoidMethod(handler_proxy, cache->handler_proxy().end_index_scan);

  DBUG_RETURN(0);
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

int HoneycombHandler::get_next_index_row(uchar* buf)
{
  JavaFrame frame(env, 1);
  jbyteArray row_bytes = static_cast<jbyteArray>(this->env->CallObjectMethod(handler_proxy, 
        cache->handler_proxy().get_next_row));
  if (row_bytes == NULL)
  {
    this->table->status = STATUS_NOT_FOUND;
    return HA_ERR_END_OF_FILE;
  }

  deserialized_from_java(row_bytes, *this->row);
  return read_row(buf, this->row);
}

int HoneycombHandler::read_row(uchar *buf, Row* row)
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
