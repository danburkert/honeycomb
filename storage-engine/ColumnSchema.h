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


#ifndef COLUMN_SCHEMA_H
#define COLUMN_SCHEMA_H

#include <avro.h>

#define COLUMN_SCHEMA "{\"type\":\"record\",\"name\":\"AvroColumnSchema\",\"fields\":[{\"name\":\"type\",\"type\":{\"type\":\"enum\",\"name\":\"ColumnType\",\"symbols\":[\"STRING\",\"BINARY\",\"ULONG\",\"LONG\",\"DOUBLE\",\"DECIMAL\",\"TIME\",\"DATE\",\"DATETIME\"]}},{\"name\":\"isNullable\",\"type\":\"boolean\",\"default\":true},{\"name\":\"isAutoIncrement\",\"type\":\"boolean\",\"default\":false},{\"name\":\"maxLength\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"scale\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"precision\",\"type\":[\"null\",\"int\"],\"default\":null}]}"

class ColumnSchema
{
  private:
    avro_schema_t column_schema_schema;
    avro_value_t column_schema;

    bool get_bool_field(const char name[]);

    int set_bool_field(const char name[], bool bool_val);

    /**
     * Gets the value of the integer fields associated with the ColumnSchema.
     * All of these fields are unions of null or the integer. This function
     * returns a negative integer to indicate a null value (the range of the
     * valid integers is non-negative). This function is only intended to be
     * used for testing, if we end up needing to check the value on the C++
     * side we should probably come up with something more robust, such as
     * bool is_auto_increment_null().
     */
    int get_int_field(const char name[]);

    int set_int_field(const char name[], int val);

    int set_null_field(const char name[]);

    int set_defaults();

  public:

    enum ColumnType
    {
      STRING,
      BINARY,
      ULONG,
      LONG,
      DOUBLE,
      DECIMAL,
      TIME,
      DATE,
      DATETIME
    };

    ColumnSchema();
    ~ColumnSchema();

    /**
     * @brief Resets the ColumnSchema to a fresh state. Resetting an existing Row
     * is much faster than creating a new one.
     * @return Error code
     */
    int reset();

    bool equals(const ColumnSchema& other);

    int serialize(const char** buf, size_t* len);

    int deserialize(const char* buf, int64_t len);

    ColumnType get_type();

    int set_type(ColumnType type);

    bool get_is_nullable();

    int set_is_nullable(bool is_nullable);

    bool get_is_auto_increment();

    int set_is_auto_increment(bool is_nullable);

    int get_max_length();

    int set_max_length(int length);

    int get_scale();

    int set_scale(int scale);

    int get_precision();

    int set_precision(int precision);

    avro_value_t* get_avro_value();

    int set_avro_value(avro_value_t*);
};
#endif
