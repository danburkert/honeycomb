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


#ifndef ROW_H
#define ROW_H

#include <avro.h>
#include "Serializable.h"

/**
 * @brief A serializable container to hold a MySQL row 
 */
class Row : public Serializable
{
  private:
    avro_schema_t row_container_schema;
    avro_value_t row_container;
    static const int CURRENT_VERSION;
    static const char* VERSION_FIELD;

  public:

  Row();
  ~Row();

  /**
   * @brief Resets the Row to a fresh state with a no UUID and an empty row map.
   *
   * Reseting an existing Row is much faster than creating a new one.
   * @return Error code
   */
  int reset();

  bool equals(const Row& other);

  /**
   * @brief Serialize Row to buf and set serialized length in len
   *
   * @param buf Pointer to a byte buffer holding the serialized Row.  The caller
   * is responsible for delete[] the buffer after finishing with it.
   * @return Error code
   */
  int serialize(const char** buf, size_t* len);

  /**
   * @brief Deserialize Row from a buffer
   *
   * @param buf Buffer of serialized row
   * @param len Length of buffer
   *
   * @return Success
   */
  int deserialize(const char* buf, int64_t len);

  /**
   * @brief set count to the number of records in the row
   *
   * @param the count
   * @return Error code
   */
  int record_count(size_t* count);

  /**
   * @brief Get the UUID of the Row
   *
   * @param buf A pointer to a char buffer.  Upon return the pointer will point
   *            to a byte buffer containing the UUID's bytes.
   *
   * @return Error code
   */
  int get_UUID(const char** buf);

  /**
   * @brief Set the UUID of the Row
   *
   * @param uuid_buf  byte buffer holding new UUID value. Must be 16 bytes long.
   *
   * @return Error code
   */
  int set_UUID(unsigned char* uuid_buf);

  /**
   * @brief Set the schema version of the Row
   *
   * @param version  The version used during serialization.
   *
   * @return Error code
   */
  int set_schema_version(const int& version);

  /**
   * @brief Get the bytes of a record in the Row.  The value byte buffer will
   * be set to NULL if the record is not in the Row.
   *
   * @param column_name   The column of the requested record
   * @param value   A pointer to the result byte buffer
   * @param size  A pointer to the size of the result byte buffer
   *
   * @return  Error code
   */
  int get_value(const char* column_name, const char** value, size_t* size);

  /**
   * @brief Set record in Row to given value and size.
   *
   * @param column_name Column of record
   * @param value Byte buffer value of record
   * @param size Size of value
   *
   * @return Error code
   */
  int set_value(const char* column_name, char* value, size_t size);
};
#endif
