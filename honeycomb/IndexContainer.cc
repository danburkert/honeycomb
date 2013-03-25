#include "IndexContainer.h"
#include "AvroUtil.h"

const char TYPE[] = "queryType";
const char RECORDS[] = "records";

IndexContainer::IndexContainer()
{
  if (avro_schema_from_json_literal(INDEX_CONTAINER_SCHEMA, &container_schema_schema))
  {
    printf("Unable to create Index Container schema.  Exiting.\n");
    abort();
  };
  avro_value_iface_t* rc_class = avro_generic_class_from_schema(container_schema_schema);
  if (avro_generic_value_new(rc_class, &container_schema))
  {
    printf("Unable to create IndexSchema.  Exiting.\n");
    abort();
  }
  avro_value_iface_decref(rc_class);
}

IndexContainer::~IndexContainer()
{
  avro_value_decref(&container_schema);
  avro_schema_decref(container_schema_schema);
}

int IndexContainer::reset()
{
  return avro_value_reset(&container_schema);
}

bool IndexContainer::equals(const IndexContainer& other)
{
  avro_value_t other_schema = other.container_schema;
  return avro_value_equal(&container_schema, &other_schema);
}

int IndexContainer::serialize(const char** buf, size_t* len)    
{
  return serialize_object(&container_schema, buf, len);
}

int IndexContainer::deserialize(const char* buf, int64_t len)
{
  return deserialize_object(&container_schema, buf, len);
}

int IndexContainer::set_bytes_record(const char* column_name, unsigned char* value, size_t size)
{
  return set_map_value(&container_schema, column_name, RECORDS, value, size);
}

int IndexContainer::get_bytes_record(const char* column_name, const unsigned char** value, size_t* size)
{
  return get_map_value(&container_schema, column_name, RECORDS, value, size);
}

IndexContainer::QueryType IndexContainer::get_type()
{
  int val;
  avro_value_t avro_enum;
  avro_value_get_by_name(&container_schema, TYPE, &avro_enum, NULL);
  avro_value_get_enum(&avro_enum, &val);
  return static_cast<QueryType>(val);
}

int IndexContainer::set_type(QueryType type)
{
  avro_value_t avro_enum;
  return avro_value_get_by_name(&container_schema, TYPE, &avro_enum, NULL) |
         avro_value_set_enum(&avro_enum, type);
}

int IndexContainer::record_count(size_t* count)
{
  int ret = 0;
  avro_value_t  map;
  ret |= avro_value_get_by_name(&container_schema, RECORDS, &map, NULL);
  ret |= avro_value_get_size(&map, count);
  return ret;
}
