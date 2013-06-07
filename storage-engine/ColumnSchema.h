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


#ifndef COLUMN_SCHEMA_H
#define COLUMN_SCHEMA_H

#include <avro.h>

#define COLUMN_SCHEMA "{\"type\":\"record\",\"name\":\"AvroColumnSchema\",\"fields\":[{\"name\":\"type\",\"type\":{\"type\":\"enum\",\"name\":\"ColumnType\",\"symbols\":[\"STRING\",\"BINARY\",\"ULONG\",\"LONG\",\"DOUBLE\",\"DECIMAL\",\"TIME\",\"DATE\",\"DATETIME\"]}},{\"name\":\"isNullable\",\"type\":\"boolean\",\"default\":true},{\"name\":\"isAutoIncrement\",\"type\":\"boolean\",\"default\":false},{\"name\":\"maxLength\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"scale\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"precision\",\"type\":[\"null\",\"int\"],\"default\":null}]}"

/**
 * @brief A container that stores metadata for a MySQL table's column. See TableSchema for more information.
 */
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

      /**
       * @brief Corresponds to MySQL VARCHAR, TEXT and CHAR data types
       */
      STRING,
      /**
       * @brief Corresponds to MySQL VARBINARY, BLOB and BINARY data types
       */
      BINARY,
      /**
       * @brief Corresponds to MySQL unsigned INTEGER, INT, SMALLINT, TINYINT, MEDIUMINT and BIGINT data types
       */
      ULONG,
      /**
       * @brief Corresponds to MySQL signed INTEGER, INT, SMALLINT, TINYINT, MEDIUMINT, BIGINT and TIMESTAMP data types
       */
      LONG,
      /**
       * @brief Corresponds to MySQL FLOAT and DOUBLE data types
       */
      DOUBLE,
      /**
       * @brief Corresponds to MySQL DECIMAL and NUMERIC data type
       */
      DECIMAL,
      /**
       * @brief Corresponds to MySQL TIME data type
       */
      TIME,
      /**
       * @brief Corresponds to MySQL DATE data type
       */
      DATE,
      /**
       * @brief Corresponds to MySQL DATETIME data type
       */
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

    /**
     * @brief Compare this ColumnSchema with another
     *
     * @param other Other ColumnSchema
     *
     * @return Equal?
     */
    bool equals(const ColumnSchema& other);

    /**
     * @brief Serialize this ColumnSchema into a buffer
     *
     * @param buf Buffer to hold the serialized data
     * @param len Length of the serialized data
     *
     * @return Success
     */
    int serialize(const char** buf, size_t* len);

    /**
     * @brief Deserialize a buffer into this ColumnSchema
     *
     * @param buf Buffer containing the serialized ColumnSchema
     * @param len Length of the data in buf
     *
     * @return Success
     */
    int deserialize(const char* buf, int64_t len);

    /**
     * @brief Retrieve the type of the column
     *
     * @return Column type
     */
    ColumnType get_type();

    /**
     * @brief Set the type of the column
     *
     * @param type Column type
     *
     * @return Success
     */
    int set_type(ColumnType type);

    /**
     * @brief Retrieve whether the column is nullable
     *
     * @return Is nullable column?
     */
    bool get_is_nullable();

    /**
     * @brief Set the column nullable
     *
     * @param is_nullable Is column nullable
     *
     * @return Success
     */
    int set_is_nullable(bool is_nullable);

    /**
     * @brief Retrieve whether the column is an auto increment 
     *
     * @return Is auto increment column?
     */
    bool get_is_auto_increment();

    /**
     * @brief Set the column as auto increment
     *
     * @param is_nullable Is auto increment
     *
     * @return Success
     */
    int set_is_auto_increment(bool is_nullable);

    /**
     * @brief Retrieve the max length of the column
     *
     * @return Max length of the column
     */
    int get_max_length();

    /**
     * @brief Set the max length of the column
     *
     * @param length Column max length
     *
     * @return Success
     */
    int set_max_length(int length);

    /**
     * @brief Retrieve the scale of the column
     *
     * @return Column scale
     */
    int get_scale();

    /**
     * @brief Set the scale of the column
     *
     * @param scale Scale of the column
     *
     * @return Success
     */
    int set_scale(int scale);

    /**
     * @brief Retrieve the precision of the column
     *
     * @return Precision of the column
     */
    int get_precision();

    /**
     * @brief Set the precision of the column
     *
     * @param precision Column precision
     *
     * @return Success
     */
    int set_precision(int precision);

    /**
     * @brief Retrieve the underlying avro object for this column
     *
     * @return Avro object
     */
    avro_value_t* get_avro_value();

    /**
     * @brief Set the underlying avro object for this column
     *
     * @param avro_value_t New avro object
     *
     * @return Success
     */
    int set_avro_value(avro_value_t*);
};
#endif
