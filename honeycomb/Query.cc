#include "HoneycombHandler.h"
#include "JavaFrame.h"
#include "Java.h"
#include "Logging.h"
#include "Macros.h"
#include "IndexContainer.h"

IndexContainer::QueryType retrieve_query_flag(enum ha_rkey_function find_flag);

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
  Row found_row;

  KEY *key_info = table->s->key_info + this->active_index;
  KEY_PART_INFO *key_part = key_info->key_part;
  KEY_PART_INFO *end_key_part = key_part + key_info->key_parts;

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
  }

  jbyteArray jserialized_key = serialize_to_java(index_key);
  this->env->CallVoidMethod(handler_proxy, cache->handler_proxy().start_index_scan, jserialized_key);
  jbyteArray row_bytes = static_cast<jbyteArray>(this->env->CallObjectMethod(handler_proxy, 
        cache->handler_proxy().get_next_row));
  if (row_bytes == NULL)
  {
    this->table->status = STATUS_NOT_FOUND;
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }

  deserialized_from_java(row_bytes, found_row);
  rc = read_row(buf, &found_row);
  DBUG_RETURN(rc);
}

IndexContainer::QueryType retrieve_query_flag(enum ha_rkey_function find_flag)
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
