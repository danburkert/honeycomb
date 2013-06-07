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


#ifndef INDEXCONTAINER_H
#define INDEXCONTAINER_H

#include <avro.h>
#include <stdlib.h>
#include "Serializable.h"

/**
 * @brief A serializable container to hold the keys in a query over an index and type of query.
 * Effectively, the QueryKey is a map of column name to predicate value.
 * For example, in the following query:
 * @code{.sql}
 * create table foo (col1 int, col2 int, index (col1,col2));
 * select * from foo where col1 = 2 and col2 = 5;
 * @endcode
 * the QueryKey would hold the value {col1 -> 2, col2 -> 5} and type EXACT_KEY. 
 */
class QueryKey : public Serializable
{
  private:
    avro_schema_t container_schema_schema;
    avro_value_t container_schema;
    int get_record(const char* column_name, const char* type, avro_value_t** entry_value);
    int set_record(const char* column_name, const char* type, avro_value_t* record);
  public:
    enum QueryType
    {

      /**
       * @brief Search for an exact match
       */
      EXACT_KEY,
      /**
       * @brief Search for everything after the key
       */
      AFTER_KEY,
      /**
       * @brief Search for an exact match or after the key
       */
      KEY_OR_NEXT,
      /**
       * @brief Search for an exact match or before the key
       */
      KEY_OR_PREVIOUS,
      /**
       * @brief Search for everything before the key
       */
      BEFORE_KEY,
      /**
       * @brief Search through the whole ascending index 
       */
      INDEX_FIRST,
      /**
       * @brief Search through the whole descending index
       */
      INDEX_LAST
    };

    QueryKey();
    
    ~QueryKey();

    int reset();

    bool equals(const QueryKey& other);

    int serialize(const char** buf, size_t* len);

    int deserialize(const char* buf, int64_t len);

    /**
     * @brief Associate a value with a column in the query key.
     *
     * @param column_name Name of the indexed column
     * @param value Value of the key
     * @param size Size of the value
     *
     * @return Success if 0 else error code
     */
    int set_value(const char* column_name, char* value, size_t size);

    /**
     * @brief Retrieve the value associated to a column in the query key.
     *
     * @param column_name Column name to look up the value with
     * @param value Value associated with the column
     * @param size Size of the value
     *
     * @return Success if 0 else error code
     */
    int get_value(const char* column_name, const char** value, size_t* size);

    /**
     * @brief Set the type of the query key. See QueryType for possible values.
     *
     * @param type Query type
     *
     * @return Success if 0 else error code
     */
    int set_type(QueryType type);

    /**
     * @brief Retrieve the type of the query key.
     *
     * @return Query type
     */
    QueryType get_type();

    int record_count(size_t* count);

    /**
     * @brief Set the name of the index containing the columns.
     *
     * @param index_name Index name
     *
     * @return Success if 0 else error code
     */
    int set_name(const char* index_name);

    /**
     * @brief Retrieve the name of the index.
     *
     * @return Index name
     */
    const char* get_name();
};

#endif 
