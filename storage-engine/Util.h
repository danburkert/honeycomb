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
bool is_unsigned_field(Field *field);

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
 * @brief Create a copy of a MySQL index key.
 *
 * @param index_field MySQL index key field
 * @param key MySQL index key
 * @param key_len Length of the key 
 * @param thd Current thread
 *
 * @return Copy of the index key
 */
uchar* create_key_copy(Field* index_field, const uchar* key, uint* key_len, THD* thd);

/**
 * @brief Extract a MYSQL_TIME from a epoch.
 *
 * @param tmp Epoch value of time
 * @param time MYSQL_TIME for the epoch
 */
void extract_mysql_time(long tmp, MYSQL_TIME *time);

/**
 * @brief Reverse the endian-ness of a uint64_t
 *
 * @param x Number
 *
 * @return Number with endian-ness reversed
 */
uint64_t bswap64(uint64_t x);

/**
 * @brief Count the number of fields in a table
 *
 * @param table Table
 *
 * @return Table field count
 */
int count_fields(TABLE* table);

/**
 * @brief Check that a path exists.
 *
 * @param path Path to check
 *
 * @return Does the path exist
 */
bool does_path_exist(const char* path);

/**
 * @brief Check whether a path is readable and writable.
 *
 * @param path Path to check
 *
 * @return Can read and write path
 */
bool can_read_write(const char* path);

/**
 * @brief Retrieve the group name of the process
 *
 * @param buffer Group name buffer
 * @param buf_size Size of the buffer
 */
void get_current_user_group(char* buffer, size_t buf_size);

/**
 * @brief Retrieve the owner and group for a file.
 *
 * @param file File to check
 * @param buffer Buffer for owner and group
 * @param buf_size Size of buffer
 */
void get_file_user_group(const char* file, char* buffer, size_t buf_size);

/**
 * @brief Format a path + file name such that it is file accessable.
 *
 * @param path Directory path
 * @param file_name File name
 *
 * @return File name appended to directory path
 */
char* format_directory_file_path(const char* path, const char* file_name);
#endif
