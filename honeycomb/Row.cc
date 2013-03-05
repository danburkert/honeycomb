#include "Row.h"
#include <stdio.h>

const char ROW_CONTAINER_SCHEMA[] = "{\"type\": \"record\", \"name\": \"RowContainer\", \"namespace\": \"com.nearinfinity.honeycomb.mysql.gen\", \"fields\": [ {\"name\": \"uuid\", \"type\": {\"type\":\"fixed\", \"name\": \"UUIDContainer\", \"size\": 16}}, {\"name\":\"records\",\"type\":{\"type\":\"map\",\"values\":[\"bytes\"],\"avro.java.string\":\"String\"}}]}";

int Row::get_record(const char* column_name, const char* type, avro_value_t** record)
{
  int ret = 0;
  int type_disc;
  int null_disc;
  int disc;
  avro_value_t records_map;
  avro_value_t record_union;
  avro_schema_t union_schema;

  // Get the records map and find the record belonging to the column
  ret |= avro_value_get_by_name(&row_container, "records", &records_map, NULL);
  ret |= avro_value_get_by_name(&records_map, column_name, &record_union, NULL);
  if (record_union.self == NULL) {*record = NULL; return 0;} // Not found

  union_schema = avro_value_get_schema(&record_union);

  // Get the discriminant (union offset) of the actual record, as well as
  // the expected type and null type discriminants
  ret |= avro_value_get_discriminant(&record_union, &disc);
  avro_schema_union_branch_by_name(union_schema, &type_disc, type);
  avro_schema_union_branch_by_name(union_schema, &null_disc, "null");

  if (!ret)
  {
    if (disc == type_disc)
    {
      ret |= avro_value_get_current_branch(&record_union, *record);
    } else if (disc == null_disc) {
      *record = NULL;
    } else {
      ret = -1;
    }
  }
  return ret;
}

int Row::set_record(const char* column_name, const char* type, avro_value_t* record)
{
  int ret = 0;
  int type_disc;
  avro_value_t records_map;
  avro_value_t record_union;
  avro_schema_t union_schema;

  ret |= avro_value_get_by_name(&row_container, "records", &records_map, NULL);
  ret |= avro_value_add(&records_map, column_name, &record_union, NULL, NULL);

  union_schema = avro_value_get_schema(&record_union);
  avro_schema_union_branch_by_name(union_schema, &type_disc, type);

  // The return value from avro_value_get_current_branch is not an error
  // code as far as I can tell.  See test_avro_value.c.  It returns 22 when
  // there is no current branch, which would be most of the time during a set
  avro_value_get_current_branch(&record_union, record);
  ret |= avro_value_set_branch(&record_union, type_disc, record);

  return ret;
}

Row::Row()
{
  if (avro_schema_from_json_literal(ROW_CONTAINER_SCHEMA, &row_container_schema))
  {
    printf("Unable to create RowContainer schema.  Exiting.\n");
    abort();
  };
  avro_value_iface_t* rc_class = avro_generic_class_from_schema(row_container_schema);
  if (avro_generic_value_new(rc_class, &row_container))
  {
    printf("Unable to create RowContainer.  Exiting.\n");
    abort();
  }
  avro_value_iface_decref(rc_class);
}

Row::~Row()
{
  avro_value_decref(&row_container);
  avro_schema_decref(row_container_schema);
}

int Row::reset()
{
  return avro_value_reset(&row_container);
}

bool Row::equal(const Row& other)
{
  avro_value_t other_row_container = other.row_container;
  return avro_value_equal(&row_container, &other_row_container);
}

int Row::record_count(size_t* count)
{
  int ret = 0;
  avro_value_t records_map;
  ret |= avro_value_get_by_name(&row_container, "records", &records_map, NULL);
  ret |= avro_value_get_size(&records_map, count);
  return ret;
}

int Row::get_UUID(const char** buf)
{
  int ret = 0;
  size_t size = 16;
  avro_value_t uuid;
  ret |= avro_value_get_by_name(&row_container, "uuid", &uuid, NULL);
  ret |= avro_value_get_fixed(&uuid, (const void**)buf, &size);
  return ret;
}

int Row::set_UUID(char* uuid_buf)
{
  int ret = 0;
  avro_value_t uuid;
  ret |= avro_value_get_by_name(&row_container, "uuid", &uuid, NULL);
  ret |= avro_value_set_fixed(&uuid, uuid_buf, 16);
  return ret;
}

int Row::get_bytes_record(const char* column_name, const char** value, size_t* size)
{
  int ret;
  avro_value_t record;
  avro_value_t* rec_ptr = &record;

  ret = get_record(column_name, "bytes", &rec_ptr);
  if (!ret && (rec_ptr == NULL))
  {
    *value = NULL;
  } else {
    ret |= avro_value_get_bytes(rec_ptr, (const void**) value, size);
  }
  return ret;
}

int Row::set_bytes_record(const char* column_name, char* value, size_t size)
{
  int ret = 0;
  avro_value_t record;
  ret |= set_record(column_name, "bytes", &record);
  ret |= avro_value_set_bytes(&record, value, size);
  return ret;
}

int Row::serialize(const char** buf, size_t* len)
{
  int ret = 0;
  ret |= avro_value_sizeof(&row_container, len);
  *buf = (const char*) malloc(sizeof(const char) * (*len));
  if(*buf)
  {
    avro_writer_t writer = avro_writer_memory(*buf, *len);
    ret |= avro_value_write(writer, &row_container);
    avro_writer_free(writer);
  } else {
    ret = -1;
  }
  return ret;
}

int Row::deserialize(const char* buf, int64_t len)
{
  int ret = 0;
  avro_reader_t reader = avro_reader_memory(buf, len);
  ret |= avro_value_read(reader, &row_container);
  avro_reader_free(reader);
  return ret;
}

