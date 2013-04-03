#include "Row.h"
#include "AvroUtil.h"
#include <stdio.h>

#define ROW_CONTAINER_SCHEMA "{\"type\":\"record\",\"name\":\"RowContainer\",\"namespace\":\"com.nearinfinity.honeycomb.mysql.gen\",\"fields\":[{\"name\":\"uuid\",\"type\":{\"type\":\"fixed\",\"name\":\"UUIDContainer\",\"size\":16}},{\"name\":\"records\",\"type\":{\"type\":\"map\",\"values\":\"bytes\",\"avro.java.string\":\"String\"}}]}"

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

bool Row::equals(const Row& other)
{
  avro_value_t other_row_container = other.row_container;
  return avro_value_equal(&row_container, &other_row_container);
}

int Row::serialize(const char** buf, size_t* len)
{
  return serialize_object(&row_container, buf, len);
}

int Row::deserialize(const char* buf, int64_t len)
{
  return deserialize_object(&row_container, buf, len);
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

int Row::set_UUID(unsigned char* uuid_buf)
{
  int ret = 0;
  avro_value_t uuid;
  ret |= avro_value_get_by_name(&row_container, "uuid", &uuid, NULL);
  ret |= avro_value_set_fixed(&uuid, uuid_buf, 16);
  return ret;
}

int Row::get_bytes_record(const char* column_name, const char** value, size_t* size)
{
  return get_map_value(&row_container, column_name, "records", value, size);
}

int Row::set_bytes_record(const char* column_name, char* value, size_t size)
{
  return set_map_value(&row_container, column_name, "records", value, size);
}
