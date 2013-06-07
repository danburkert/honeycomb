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


#ifndef AVRO_UTIL_H
#define AVRO_UTIL_H

#include <cstdlib>

struct avro_value;
typedef avro_value avro_value_t;


/**
 * @brief Convert an Avro object to a byte string
 *
 * @param obj Avro object to serialize
 * @param buf Buffer to store the serialize Avro object
 * @param len Length of the serialized Avro object
 *
 * @return Success
 */
int serialize_object(avro_value_t* obj, const char** buf, size_t* len);

/**
 * @brief Convert a byte string into an Avro object
 *
 * @param obj Avro object to store values
 * @param buf Byte string containing the serialized Avro object
 * @param len Length of buf
 *
 * @return Success
 */
int deserialize_object(avro_value_t* obj, const char* buf, int64_t len);

/**
 * @brief Get a value from an Avro map
 * i.e. return map[entry_key]
 *
 * @param schema Schema containing a map
 * @param entry_key Key for the map entry
 * @param map_name Name of the map in the schema
 * @param value Container to store the value retrieved from the map [Out] 
 * @param size Size of the value retrieved from the map [Out]
 *
 * @return Success
 */
int get_map_value(avro_value_t* schema, const char* entry_key, const char* map_name, const char** value, size_t* size);

/**
 * @brief Set an entry in an Avro map to a value.
 * i.e. map[entry_key] = value
 *
 * @param schema Schema containing a map
 * @param entry_key Key for the map entry
 * @param map_name Name of the map in the schema
 * @param value Value to set the entry to
 * @param size Size of the value
 *
 * @return Success
 */
int set_map_value(avro_value_t* schema, const char* entry_key, const char* map_name, char* value, size_t size);

#endif
