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


#ifndef TABLE_SCHEMA_H
#define TABLE_SCHEMA_H

#include <avro.h>
#include "Serializable.h"

class ColumnSchema;
class IndexSchema;

#define TABLE_SCHEMA "{\"type\":\"record\",\"name\":\"AvroTableSchema\",\"namespace\":\"com.nearinfinity.honeycomb.mysql.gen\",\"fields\":[{\"name\":\"version\",\"type\":\"int\",\"doc\":\"Schema version number\",\"default\":0},{\"name\":\"columns\",\"type\":{\"type\":\"map\",\"values\":" COLUMN_SCHEMA ",\"avro.java.string\":\"String\"}},{\"name\":\"indices\",\"type\":{\"type\":\"map\",\"values\":" INDEX_SCHEMA ",\"avro.java.string\":\"String\"}}]}"

/**
 * @brief A serializable container that stores metadata for a MySQL table's indices and columns.
 * For example, a MySQL table:
 * @code{.sql}
 * create table foo (x int, index (x));
 * @endcode
 * will have a TableSchema containing one ColumnSchema and IndexSchema. The ColumnSchema will contain information such
 * as the type \c int and column name \c x. The IndexSchema will contain information such as a uniqueness constraint
 * and column name.
 */
class TableSchema : public Serializable
{
  private:
    avro_schema_t table_schema_schema;
    avro_value_t table_schema;
    static const int CURRENT_VERSION;
    static const char* VERSION_FIELD;

    int add_to_map_field(const char* field_name, const char* key, avro_value_t* value);
    int get_from_map_field(const char* field_name, const char* key, avro_value_t* value);

    /**
     * @brief Set the schema version of the table schema
     * @param version  The version used during serialization.
     * @return Error code
     */
    int set_schema_version(const int& version);

  public:
    TableSchema();

    ~TableSchema();

    int reset();

    bool equals(const TableSchema& other);

    int serialize(const char** buf, size_t* len);

    int deserialize(const char* buf, int64_t len);

    int add_column(const char* name, ColumnSchema* schema);

    int get_column(const char* name, ColumnSchema* schema);

    int add_index(const char* name, IndexSchema* schema);

    int get_index(const char* name, IndexSchema* schema);

    size_t column_count();

    size_t index_count();
};
#endif
