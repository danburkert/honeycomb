#include "HoneycombHandler.h"
#include "JavaFrame.h"
#include "Java.h"
#include "Logging.h"
#include "Macros.h"
#include "IndexContainer.h"

static int retrieve_query_flag(enum ha_rkey_function find_flag, IndexContainer::QueryType* query_type);

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
  IndexContainer::QueryType query_type;
  int rc = retrieve_query_flag(find_flag, &query_type);
  if (rc) { DBUG_RETURN(rc); }

  uchar* key_ptr = (uchar*)key;

  KEY *key_info = table->s->key_info + this->active_index;
  KEY_PART_INFO *key_part = key_info->key_part;
  KEY_PART_INFO *end_key_part = key_part + key_info->key_parts;

  index_key.set_type(query_type);
  index_key.set_name(key_info->name);
  while (key_part < end_key_part && keypart_map)
  {
    uint key_length;
    Field* field = key_part->field;
    key_length = field->pack_length();
    bool is_null_field = field->is_real_null();
    if (is_null_field && key_ptr[0] == 1) // Key is nullable and is actually null
    {
      // Absence is the indicator of null on index key
      key_ptr += key_part->store_length;
      key_part++;
      keypart_map >>= 1;
      continue;
    }

    // If it is a null field then we have to move past the null byte.
    uchar* key_offset = is_null_field ? key_ptr + 1 : key_ptr;
    uchar* key_copy = create_key_copy(field, key_offset, &key_length, table->in_use);
    index_key.set_record(field->field_name, (char*)key_copy, key_length);
    ARRAY_DELETE(key_copy);
    key_ptr += key_part->store_length;
    key_part++;
    keypart_map >>= 1;
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
  DBUG_ENTER("HoneycombHandler::rnd_pos");
  int rc = 0;
  ha_statistic_increment(&SSV::ha_read_rnd_count); // Boilerplate

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

  DBUG_RETURN(rc);
}

int HoneycombHandler::rnd_next(uchar *buf)
{
  DBUG_ENTER("HoneycombHandler::rnd_next");

  ha_statistic_increment(&SSV::ha_read_rnd_next_count);
  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str, TRUE);

  int rc = get_next_row(buf);

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
  jbyteArray jserialized_key = serialize_to_java(env, index_key);
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

  deserialize_from_java(env, row_bytes, *this->row);
  return read_row(buf);
}

int HoneycombHandler::read_row(uchar *buf)
{
  store_uuid_ref(this->row);

  if (unpack_row(buf, this->row))
  {
    this->table->status = STATUS_NOT_READ;
    return HA_ERR_INTERNAL_ERROR;
  }

  this->table->status = 0;
  return 0;
}

/**
 * @brief Turns a MySQL index flag into an avro flag. If the index flag is not supported
 * it will return generic error code.
 *
 * @param find_flag MySQL index search flag
 * @param query_type Corresponding avro flag [out]
 *
 * @return Success 0 otherwise HA_ERR_GENERIC
 */
static int retrieve_query_flag(enum ha_rkey_function find_flag, IndexContainer::QueryType* query_type)
{
  switch(find_flag)
  {
    case HA_READ_KEY_EXACT:
      *query_type = IndexContainer::EXACT_KEY;
      break;
    case HA_READ_KEY_OR_NEXT:
      *query_type = IndexContainer::KEY_OR_NEXT;
      break;
    case HA_READ_KEY_OR_PREV:
    case HA_READ_PREFIX_LAST_OR_PREV:
      *query_type = IndexContainer::KEY_OR_PREVIOUS;
      break;
    case HA_READ_AFTER_KEY:
      *query_type = IndexContainer::AFTER_KEY;
      break;
    case HA_READ_BEFORE_KEY:
      *query_type = IndexContainer::BEFORE_KEY;
      break;
    default:
      return HA_ERR_GENERIC;
  }
  return 0;
}

/**
 * @brief Converts an Avro row into the MySQL unireg row format.
 *
 * @param buf MySQL unireg row buffer
 * @param row Avro row
 * @return 0 on success
 */
int HoneycombHandler::unpack_row(uchar* buf, Row* row)
{
  my_bitmap_map *orig_bitmap;
  orig_bitmap = dbug_tmp_use_all_columns(table, table->write_set);
  const char* value;
  size_t size;

  for (uint i = 0; i < table->s->fields; i++)
  {
    Field *field = table->field[i];
    const char* key = field->field_name;
    row->get_bytes_record(key, &value, &size);
    if (value == NULL)
    {
      field->set_null();
      continue;
    }

    my_ptrdiff_t offset = (my_ptrdiff_t) (buf - this->table->record[0]);
    field->move_field_offset(offset);

    field->set_notnull(); // for some reason the field was inited as null during rnd_pos
    store_field_value(field, value, size);

    field->move_field_offset(-offset);
  }

  dbug_tmp_restore_column_map(table->write_set, orig_bitmap);
  return 0;
}

/**
 * @brief Stores a raw byte array value into the field in MySQL format.
 *
 * @param field to store value in
 * @param val value
 * @param val_length Length of the value's buffer
 */
void HoneycombHandler::store_field_value(Field *field, const char *val, int val_length)
{
  enum_field_types type = field->real_type();

  if (!is_unsupported_field(type))
  {
    if (is_integral_field(type))
    {
      if (type == MYSQL_TYPE_LONGLONG)
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
      field->store((char*)val, val_length, &my_charset_bin);
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
        str_to_datetime((char*)val, val_length, &mysql_time, TIME_FUZZY_DATE, &was_cut);
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
