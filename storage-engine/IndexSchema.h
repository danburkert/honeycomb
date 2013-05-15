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


#ifndef INDEX_SCHEMA_H
#define INDEX_SCHEMA_H

#include <avro.h>
#include "Serializable.h"

#define INDEX_SCHEMA "{\"type\":\"record\",\"name\":\"AvroIndexSchema\",\"fields\":[{\"name\":\"columns\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}},{\"name\":\"isUnique\",\"type\":\"boolean\",\"default\":false}]}"

class IndexSchema : public Serializable
{
  private:
    avro_schema_t index_schema_schema;
    avro_value_t index_schema;

  public:
    IndexSchema();
    ~IndexSchema();

    /**
     * @brief Resets the IndexSchema to a fresh state. Reseting an existing
     * IndexSchema is much faster than creating a new one.
     * @return Error code
     */
    int reset();

    bool equals(const IndexSchema& other);

    int serialize(const char** buf, size_t* len);

    int deserialize(const char* buf, int64_t len);

    bool get_is_unique();

    int set_is_unique(bool is_unique);

    /**
     * Return the number of columns in the index schema.
     */
    size_t size();

    /**
     * Return the nth column of the index.
     */
    const char* get_column(size_t n);

    /**
     * Add a column to the index.
     */
    int add_column(const char* column_name);

    avro_value_t* get_avro_value();

    int set_avro_value(avro_value_t*);
};
#endif
