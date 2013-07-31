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


#ifndef UTIL_H
#define UTIL_H

#ifndef MYSQL_SERVER
#define MYSQL_SERVER 1
#endif

#include "sql_class.h"
#include <stdint.h>

/**
 * @brief Check if a field is unsigned.
 * Only applicable for a numeric field i.e. int, tinyint.
 *
 * @param field Field
 *
 * @return Is a unsigned field
 */
bool is_unsigned_field(Field& field);

/**
 * @brief Reverse the bytes of a buffer
 *
 * @param begin Buffer
 * @param length Length of buffer
 */
void reverse_bytes(uchar *begin, uint length);

/**
 * @brief Check the endianness of the platform
 *
 * @return Is the platform little endian?
 */
bool is_little_endian();

/**
 * @brief Convert a buffer to big endian
 *
 * @param begin Buffer
 * @param length Length of buffer
 */
void make_big_endian(uchar *begin, uint length);

/**
 * @brief Retrieve the table name from a MySQL table path
 *
 * @param path Table path in the form ./table-name
 *
 * @return Table name i.e. "table-name"
 */
const char* extract_table_name_from_path(const char *path);

/**
 * @brief Reverse the endian-ness of a uint64_t
 *
 * @param x Number
 *
 * @return Number with endian-ness reversed
 */
uint64_t bswap64(uint64_t x);

#endif
