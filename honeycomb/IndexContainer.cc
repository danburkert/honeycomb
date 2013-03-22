#include "IndexContainer.h"
#include "AvroUtil.h"

const char TYPE[] = "queryType";
const char RECORDS[] = "records";

int IndexContainer::get_record(const char* column_name, avro_value_t** record)
{
  int ret = 0;
  avro_value_t map;

  // Retrieve map
  ret |= avro_value_get_by_name(&container_schema, RECORDS, &map, NULL);
  // Retrieve value from map
  ret |= avro_value_get_by_name(&map, column_name, *record, NULL);
  return ret;
}

int IndexContainer::set_record(const char* column_name, const char* type, avro_value_t* record)
{
  int ret = 0;
  avro_value_t record_map;
  // Retrieve map
  ret |= avro_value_get_by_name(&container_schema, RECORDS, &record_map, NULL);
  // Add value to map
  ret |= avro_value_add(&record_map, column_name, record, NULL, NULL);
  return ret;
}

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

int IndexContainer::set_bytes_record(const char* column_name, char* value, size_t size)
{
  int ret = 0;
  avro_value_t record;
  ret |= set_record(column_name, "bytes", &record);
  ret |= avro_value_set_bytes(&record, value, size);
  return ret;
}

int IndexContainer::get_bytes_record(const char* column_name, const char** value, size_t* size)
{
  int ret = 0;
  avro_value_t record;
  avro_value_t* rec_ptr = &record;

  ret |= get_record(column_name, &rec_ptr);
  if (rec_ptr->self == NULL)
  {
    *value = NULL;
  }else
  {
    ret |= avro_value_get_bytes(rec_ptr, (const void**) value, size);
  }
  return ret;
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
