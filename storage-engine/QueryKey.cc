/*
 * Copyright (C) 2013 Altamira Corporation
 *
 * This file is part of Honeycomb Storage Engine.
 *
 * Honeycomb Storage Engine is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * Honeycomb Storage Engine is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Honeycomb Storage Engine.  If not, see <http://www.gnu.org/licenses/>.
 */


#include "QueryKey.h"
#include "AvroUtil.h"

const char TYPE[] = "queryType";
const char RECORDS[] = "records";
const char INDEX_NAME[] = "indexName";

#define INDEX_CONTAINER_SCHEMA "{\"type\":\"record\",\"name\":\"AvroQueryKey\",\"namespace\":\"com.nearinfinity.honeycomb.mysql.gen\",\"fields\":[{\"name\":\"indexName\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"queryType\",\"type\":{\"type\":\"enum\",\"name\":\"QueryType\",\"symbols\":[\"EXACT_KEY\",\"AFTER_KEY\",\"KEY_OR_NEXT\",\"KEY_OR_PREVIOUS\",\"BEFORE_KEY\",\"INDEX_FIRST\",\"INDEX_LAST\"]}},{\"name\":\"records\",\"type\":{\"type\":\"map\",\"values\":[\"null\",\"bytes\"],\"avro.java.string\":\"String\"}}]}"

int QueryKey::get_record(const char* column_name, const char* type, avro_value_t** entry_value)
{
  int ret = 0;
  int type_disc;
  int null_disc;
  int current_disc;
  avro_value_t records_map;
  avro_value_t entry_union;
  avro_schema_t union_schema;

  // Get the records map
  ret |= avro_value_get_by_name(&container_schema, "records", &records_map, NULL);
  // Get the entry associated with the column name
  ret |= avro_value_get_by_name(&records_map, column_name, &entry_union, NULL);
  // The whole union entry is null
  if (entry_union.self == NULL) {*entry_value = NULL; return 0;} // Not found

  union_schema = avro_value_get_schema(&entry_union);

  // Get the discriminant (union offset) of the actual record, as well as
  // the expected type and null type discriminants
  ret |= avro_value_get_discriminant(&entry_union, &current_disc);
  // Retrieve the name of the bytes branch
  avro_schema_union_branch_by_name(union_schema, &type_disc, type);
  // Retrieve the name of the null branch
  avro_schema_union_branch_by_name(union_schema, &null_disc, "null");

  if (!ret)
  {
    if (current_disc == type_disc)
    {
      // Extract the value object of the current union branch
      ret |= avro_value_get_current_branch(&entry_union, *entry_value);
    } else if (current_disc == null_disc) {
      // The union is on the null branch
      *entry_value = NULL;
    } else {
      ret = -1;
    }
  }
  return ret;
}

int QueryKey::set_record(const char* column_name, const char* type, avro_value_t* record)
{
  int ret = 0;
  int type_disc;
  avro_value_t records_map;
  avro_value_t entry_union;
  avro_schema_t union_schema;

  // Get the records map
  ret |= avro_value_get_by_name(&container_schema, "records", &records_map, NULL);
  // Get the entry associated with the column name
  ret |= avro_value_add(&records_map, column_name, &entry_union, NULL, NULL);

  union_schema = avro_value_get_schema(&entry_union);
  // Get the union branch for the specified type
  avro_schema_union_branch_by_name(union_schema, &type_disc, type);
  // Set the union to the branch specified by type
  ret |= avro_value_set_branch(&entry_union, type_disc, record);

  return ret;
}

QueryKey::QueryKey()
: container_schema_schema(),
  container_schema()
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

QueryKey::~QueryKey()
{
  avro_value_decref(&container_schema);
  avro_schema_decref(container_schema_schema);
}

int QueryKey::reset()
{
  return avro_value_reset(&container_schema);
}

bool QueryKey::equals(const QueryKey& other)
{
  avro_value_t other_schema = other.container_schema;
  return avro_value_equal(&container_schema, &other_schema);
}

int QueryKey::serialize(const char** buf, size_t* len)
{
  return serialize_object(&container_schema, buf, len);
}

int QueryKey::deserialize(const char* buf, int64_t len)
{
  return deserialize_object(&container_schema, buf, len);
}

int QueryKey::set_value(const char* column_name, char* value, size_t size)
{
  int ret = 0;
  avro_value_t record;
  if (value == NULL)
  {
    ret |= set_record(column_name, "null", &record);
  }
  else
  {
    ret |= set_record(column_name, "bytes", &record);
    ret |= avro_value_set_bytes(&record, value, size);
  }
  return ret;
}

int QueryKey::get_value(const char* column_name, const char** value, size_t* size)
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

QueryKey::QueryType QueryKey::get_type()
{
  int val;
  avro_value_t avro_enum;
  avro_value_get_by_name(&container_schema, TYPE, &avro_enum, NULL);
  avro_value_get_enum(&avro_enum, &val);
  return static_cast<QueryType>(val);
}

int QueryKey::set_type(QueryType type)
{
  avro_value_t avro_enum;
  return avro_value_get_by_name(&container_schema, TYPE, &avro_enum, NULL) |
         avro_value_set_enum(&avro_enum, type);
}

int QueryKey::record_count(size_t* count)
{
  int ret = 0;
  avro_value_t  map;
  ret |= avro_value_get_by_name(&container_schema, RECORDS, &map, NULL);
  ret |= avro_value_get_size(&map, count);
  return ret;
}

int QueryKey::set_name(const char* index_name)
{
  int ret = 0;
  avro_value_t record;
  ret |= avro_value_get_by_name(&container_schema, INDEX_NAME, &record, NULL);
  ret |= avro_value_set_string(&record, index_name);
  return ret;
}

const char* QueryKey::get_name()
{
  const char* result;
  avro_value_t record;
  avro_value_get_by_name(&container_schema, INDEX_NAME, &record, NULL);
  avro_value_get_string(&record, &result, NULL);
  return result;
}
