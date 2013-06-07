/*
 * Copyright (C) 2013 Near Infinity Corporation
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


#include "TableSchema.h"
#include "AvroUtil.h"
#include "IndexSchema.h"
#include "ColumnSchema.h"
#include <cstdio>

const char INDICES_MAP[] = "indices";
const char COLUMNS_MAP[] = "columns";

const int TableSchema::CURRENT_VERSION = 0;
const char* TableSchema::VERSION_FIELD = "version";

int TableSchema::add_to_map_field(const char* field_name, const char* key, avro_value_t* value)
{
  int ret = 0;
  avro_value_t map;
  avro_value_t element;

  ret |= avro_value_get_by_name(&table_schema, field_name, &map, NULL);
  ret |= avro_value_add(&map, key, &element, NULL, NULL);
  ret |= avro_value_copy(&element, value);
  return ret;
};


int TableSchema::get_from_map_field(const char* field_name, const char* key, avro_value_t* value)
{
  int ret = 0;
  avro_value_t map;

  ret |= avro_value_get_by_name(&table_schema, field_name, &map, NULL);
  ret |= avro_value_get_by_name(&map, key, value, NULL);
  if (value == NULL) { return -1; } // Not found

  return ret;
};

TableSchema::TableSchema()
: table_schema_schema(),
  table_schema()
{
  if (avro_schema_from_json_literal(TABLE_SCHEMA, &table_schema_schema))
  {
    printf("Unable to create TableSchema schema.  Exiting.\n");
    abort();
  };
  avro_value_iface_t* rc_class = avro_generic_class_from_schema(table_schema_schema);
  if (avro_generic_value_new(rc_class, &table_schema))
  {
    printf("Unable to create TableSchema.  Exiting.\n");
    abort();
  }

  set_schema_version(CURRENT_VERSION);
  avro_value_iface_decref(rc_class);
}

TableSchema::~TableSchema()
{
  avro_value_decref(&table_schema);
  avro_schema_decref(table_schema_schema);
}

int TableSchema::reset()
{
  int ret = 0;
  ret |= avro_value_reset(&table_schema);
  ret |= set_schema_version(CURRENT_VERSION);
  return ret;
}

bool TableSchema::equals(const TableSchema& other)
{
  avro_value_t other_table_schema = other.table_schema;
  return avro_value_equal(&table_schema, &other_table_schema);
}

int TableSchema::serialize(const char** buf, size_t* len)
{
  return serialize_object(&table_schema, buf, len);
}

int TableSchema::deserialize(const char* buf, int64_t len)
{
  return deserialize_object(&table_schema, buf, len);
}

int TableSchema::add_column(const char* name, ColumnSchema* schema)
{
  return add_to_map_field(COLUMNS_MAP, name, schema->get_avro_value());
};

int TableSchema::get_column(const char* name, ColumnSchema* column_schema)
{
  int ret = 0;
  avro_value_t column;

  ret = get_from_map_field(COLUMNS_MAP, name, &column);
  ret |= column_schema->set_avro_value(&column);
  return ret;
};


int TableSchema::set_schema_version(const int& version)
{
  int ret = 0;
  avro_value_t schemaVersion;
  ret |= avro_value_get_by_name(&table_schema, VERSION_FIELD, &schemaVersion, NULL);
  ret |= avro_value_set_int(&schemaVersion, version);
  return ret;
}

int TableSchema::add_index(const char* name, IndexSchema* schema)
{
  return add_to_map_field(INDICES_MAP, name, schema->get_avro_value());
};

int TableSchema::get_index(const char* name, IndexSchema* index_schema)
{
  int ret = 0;
  avro_value_t index;

  ret = get_from_map_field(INDICES_MAP, name, &index);
  ret |= index_schema->set_avro_value(&index);
  return ret;
};

size_t TableSchema::column_count()
{
  avro_value_t columns;
  size_t count;
  avro_value_get_by_name(&table_schema, "columns", &columns, NULL);
  avro_value_get_size(&columns, &count);
  return count;
}

size_t TableSchema::index_count()
{
  avro_value_t indices;
  size_t count;
  avro_value_get_by_name(&table_schema, "indices", &indices, NULL);
  avro_value_get_size(&indices, &count);
  return count;
}
