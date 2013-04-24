#include "HoneycombHandler.h"
#include "TableSchema.h"
#include "ColumnSchema.h"
#include "IndexSchema.h"
#include "JNISetup.h"
#include "Java.h"
#include "Macros.h"
#include "JavaFrame.h"
#include "JNICache.h"

/**
 * @brief Stop creation of table and return message to user.
 */
#define ABORT_CREATE(message) \
  do { \
    errno = ER_CANT_CREATE_TABLE; \
    my_errno = errno; \
    my_printf_error(ER_CANT_CREATE_TABLE, "Can't create table '%s'", MYF(0), message); \
    DBUG_RETURN(HA_WRONG_CREATE_OPTION); \
  } while(0)

const int YEAR2_NOT_SUPPORTED = 0;
const int ODD_TYPES_NOT_SUPPORTED = 1;
const int UTF_REQUIRED = 2;
const char* table_creation_errors[] = {
  "YEAR(2) is not supported.",
  "Bit, set and geometry are not supported.",
  "Required: character set utf8 collate utf8_bin"
};

/**
 * Called by MySQL during CREATE TABLE statements.  Converts the table's
 * schema into a TableSchema object and hands it off to the HandlerProxy.
 *
 * @param path  path to file MySQL assumes we will use.  We use it to extract
 *              the database/tablename.
 * @param table TABLE object associated with this thread and query.  Holds most
 *              of the information we need.  See sql/table.h.
 * @param create_info contains info specified during table creation such as
 *                    initial auto_increment value.
 */
int HoneycombHandler::create(const char *path, TABLE *table,
    HA_CREATE_INFO *create_info)
{
  const char* location = "HoneycombHandler::create";
  DBUG_ENTER(location);
  attach_thread(jvm, &env, location);

  if(table->part_info != NULL)
  {
    ABORT_CREATE("Partitions are not supported.");
  }

  int ret = 0;
  { // Destruct frame before calling detach_thread
    JavaFrame frame(env, 3);

    TableSchema table_schema;
    ColumnSchema column_schema;
    IndexSchema index_schema;

    for (Field **field_ptr = table->field; *field_ptr; field_ptr++)
    {
      Field* field = *field_ptr;
      int error_number;
      if(!is_allowed_column(field, &error_number))
      {
        ABORT_CREATE(table_creation_errors[error_number]);
      }

      column_schema.reset();
      if (pack_column_schema(&column_schema, field))
      {
        ABORT_CREATE("Error while creating column schema.");
      }
      table_schema.add_column(field->field_name, &column_schema);
    }

    for (uint i = 0; i < table->s->keys; i++)
    {
      if (pack_index_schema(&index_schema, &table->key_info[i]))
      {
        ABORT_CREATE("Error while creating index schema.");
      }
      table_schema.add_index(table->key_info[i].name, &index_schema);
    }

    jstring jtable_name = string_to_java_string(env, extract_table_name_from_path(path));
    jlong jauto_inc_value = max(1, create_info->auto_increment_value);

    jbyteArray jserialized_schema = serialize_to_java(env, table_schema);

    this->env->CallVoidMethod(handler_proxy, cache->handler_proxy().create_table,
        jtable_name, jserialized_schema, jauto_inc_value);
    ret |= check_exceptions(env, cache, "HoneycombHandler::create_table");
  }
  detach_thread(jvm);
  DBUG_RETURN(ret);
}

/**
 * Test if the column is valid in a Honeycomb table.
 */
bool HoneycombHandler::is_allowed_column(Field* field, int* error_number)
{
  bool allowed = true;
  switch (field->real_type())
  {
    case MYSQL_TYPE_YEAR:
      if (field->field_length == 2)
      {
        *error_number = YEAR2_NOT_SUPPORTED;
        allowed = false;
      }
      break;
    case MYSQL_TYPE_BIT:
    case MYSQL_TYPE_SET:
    case MYSQL_TYPE_GEOMETRY:
      *error_number = ODD_TYPES_NOT_SUPPORTED;
      allowed = false;
      break;
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_BLOB:
      if (strncmp(field->charset()->name, "utf8_bin", 8) != 0
          && field->binary() == false)
      {
        *error_number = UTF_REQUIRED;
        allowed = false;
      }
      break;
    default:
      break;
  }
  return allowed;
}

/**
 * Add column to the column schema.
 *
 * @param schema  The schema being filled out
 * @param field   The column that is being added to the schema
 */
int HoneycombHandler::pack_column_schema(ColumnSchema* schema, Field* field)
{
  int ret = 0;
  switch (field->real_type())
  {
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_LONGLONG:
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_YEAR:
      if (is_unsigned_field(field))
      {
        ret |= schema->set_type(ColumnSchema::ULONG);
      }
      else
      {
        ret |= schema->set_type(ColumnSchema::LONG);
      }
      break;
    case MYSQL_TYPE_FLOAT:
    case MYSQL_TYPE_DOUBLE:
      ret |= schema->set_type(ColumnSchema::DOUBLE);
      break;
    case MYSQL_TYPE_DECIMAL:
    case MYSQL_TYPE_NEWDECIMAL:
      {
        uint precision = ((Field_new_decimal*) field)->precision;
        uint scale = ((Field_new_decimal*) field)->dec;
        ret |= schema->set_type(ColumnSchema::DECIMAL);
        ret |= schema->set_precision(precision);
        ret |= schema->set_scale(scale);
      }
      break;
    case MYSQL_TYPE_DATE:
    case MYSQL_TYPE_NEWDATE:
      ret |= schema->set_type(ColumnSchema::DATE);
      break;
    case MYSQL_TYPE_TIME:
      ret |= schema->set_type(ColumnSchema::TIME);
      break;
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_TIMESTAMP:
      ret |= schema->set_type(ColumnSchema::DATETIME);
      break;
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_VARCHAR:
      {
        long long max_char_length = (long long) field->field_length;
        ret |= schema->set_max_length(max_char_length);

        if (field->binary())
        {
          ret |= schema->set_type(ColumnSchema::BINARY);
        }
        else
        {
          ret |= schema->set_type(ColumnSchema::STRING);
        }
      }
      break;
    case MYSQL_TYPE_BLOB:
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
      ret |= schema->set_type(ColumnSchema::BINARY);
      break;
    case MYSQL_TYPE_ENUM:
      ret |= schema->set_type(ColumnSchema::ULONG);
      break;
    case MYSQL_TYPE_NULL:
    case MYSQL_TYPE_BIT:
    case MYSQL_TYPE_SET:
    case MYSQL_TYPE_GEOMETRY:
    case MYSQL_TYPE_VAR_STRING:
    default:
      break;
  }

  if (field->real_maybe_null())
  {
    ret |= schema->set_is_nullable(true);
  }

  if(field->table->found_next_number_field != NULL
      && field == field->table->found_next_number_field)
  {
    ret |= schema->set_is_auto_increment(true);
  }
  return ret;
};

/**
 * Add columns in the index to the index schema.
 *
 * @param schema  The schema being filled out
 * @param key   The index whose schema is being copied.
 */
int HoneycombHandler::pack_index_schema(IndexSchema* schema, KEY* key)
{
  int ret = 0;
  ret |= schema->reset();
  for (uint i = 0; i < key->key_parts; i++)
  {
    ret |= schema->add_column(key->key_part[i].field->field_name);
  }
  if (key->flags & HA_NOSAME)
  {
    ret |= schema->set_is_unique(true);
  }
  return ret;
};

int HoneycombHandler::delete_table(const char *path)
{
  const char* location = "HoneycombHandler::delete_table";
  DBUG_ENTER(location);
  int ret = 0;

  attach_thread(jvm, &env, location);
  { // destruct frame before detaching
    JavaFrame frame(env, 2);
    jstring table_name = string_to_java_string(env,
        extract_table_name_from_path(path));
    this->env->CallVoidMethod(handler_proxy, cache->handler_proxy().drop_table,
        table_name);
    ret |= check_exceptions(env, cache, location);
  }
  detach_thread(jvm);

  DBUG_RETURN(ret);
}

int HoneycombHandler::rename_table(const char *from, const char *to)
{
  const char* location = "HoneycombHandler::rename_table";
  DBUG_ENTER(location);
  int ret = 0;

  attach_thread(jvm, &env, location);
  {
    JavaFrame frame(env, 2);
    jstring old_table_name = string_to_java_string(env,
        extract_table_name_from_path(from));
    jstring new_table_name = string_to_java_string(env,
        extract_table_name_from_path(to));

    env->CallVoidMethod(handler_proxy, cache->handler_proxy().rename_table,
        old_table_name, new_table_name);
    ret |= check_exceptions(env, cache, location);
  }
  detach_thread(jvm);

  DBUG_RETURN(ret);
}

/**
 * Initialize table share to table located at path.  This should only be used
 * when table share access is needed and the table share off of the
 * HoneycombHandler is uninitialized.  The caller must clean up the returned
 * TABLE_SHARE by calling free_table_share(&table_share).
 *
 * @param table_share pointer to uninitialized table share
 * @param path  path to table
 * @return error code
 */
int HoneycombHandler::init_table_share(TABLE_SHARE* table_share, const char* path)
{
  THD* thd = ha_thd();
  init_tmp_table_share(thd, table_share, "", 0, "", path);
  return open_table_def(thd, table_share, 0);
}

/**
 * Called during alter table statements by the optimizer.
 */
void HoneycombHandler::update_create_info(HA_CREATE_INFO* create_info)
{
  const char* location = "HoneycombHandler::update_create_info";
  DBUG_ENTER(location);

  //show create table
  if (!(create_info->used_fields & HA_CREATE_USED_AUTO)) {
    HoneycombHandler::info(HA_STATUS_AUTO);
    create_info->auto_increment_value = stats.auto_increment_value;
  }
  //alter table
  else if (create_info->used_fields == 1) {
    env->CallVoidMethod(handler_proxy, cache->handler_proxy().set_auto_increment,
        create_info->auto_increment_value);
    check_exceptions(env, cache, location);
  }

  DBUG_VOID_RETURN;
}

/**
 * Called by MySQL to determine if an alter table command requires a full table
 * rebuild.  Return COMPATIBLE_DATA_NO to require a rebuild.
 */
bool HoneycombHandler::check_if_incompatible_data(HA_CREATE_INFO *create_info,
    uint table_changes)
{
  // unclear what table_changes means.  When in doubt, copy inno.
  if (table_changes != IS_EQUAL_YES)
  {
    return COMPATIBLE_DATA_NO;
  }

  // If a column is renamed we are forced to rebuild, because we are not given
  // enough information to simply rename the column (AFAIK we are not given the
  // new name).
  if (this->check_column_being_renamed(table))
  {
    return COMPATIBLE_DATA_NO;
  }

  /* Check that row format didn't change */
  if ((create_info->used_fields & HA_CREATE_USED_ROW_FORMAT)
      && create_info->row_type != ROW_TYPE_DEFAULT
      && create_info->row_type != get_row_type())
  {
    return COMPATIBLE_DATA_NO;
  }

  return COMPATIBLE_DATA_YES;
}

/**
 * Check if a column is being renamed during an alter table operation
 */
bool HoneycombHandler::check_column_being_renamed(const TABLE* table)
{
  const Field* field;
  for (uint i = 0; i < table->s->fields; i++)
  {
    field = table->field[i];
    if (field->flags & FIELD_IS_RENAMED)
    {
      return true;
    }
  }
  return false;
}

int HoneycombHandler::add_index(TABLE *table_arg, KEY *key_info, uint num_of_keys,
    handler_add_index **add)
{
  const char* location = "HoneycombHandler::add_index";
  DBUG_ENTER(location);
  attach_thread(jvm, &env, location);
  int ret = 0;
  IndexSchema schema;
  for(uint k = 0; k < num_of_keys; k++)
  {
    JavaFrame frame(env, 2);
    KEY* key = key_info + k;
    jstring index_name = string_to_java_string(env, key->name);
    pack_index_schema(&schema, key);

    if (key->flags & HA_NOSAME)
    {
      // We don't support adding unique indices without a table rebuild
      DBUG_RETURN(HA_ERR_WRONG_COMMAND);
    }

    jbyteArray serialized_schema = serialize_to_java(env, schema);

    this->env->CallVoidMethod(handler_proxy, cache->handler_proxy().add_index,
        index_name, serialized_schema);
    ret |= check_exceptions(env, cache, location);
  }
  detach_thread(jvm);
  DBUG_RETURN(ret);
}

int HoneycombHandler::prepare_drop_index(TABLE *table, uint *key_num, uint num_of_keys)
{
  const char* location = "HoneycombHandler::prepare_drop_index";
  DBUG_ENTER(location);
  attach_thread(jvm, &env, location);
  int ret = 0;
  for (uint i = 0; i < num_of_keys; i++) {
    JavaFrame frame(env, 1);
    jstring index_name = string_to_java_string(env, (table->key_info + key_num[i])->name);
    this->env->CallVoidMethod(handler_proxy, cache->handler_proxy().drop_index,
        index_name);
    ret |= check_exceptions(env, cache, location);
  }
  detach_thread(jvm);
  DBUG_RETURN(ret);
}
