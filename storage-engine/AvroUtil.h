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


#ifndef AVRO_UTIL_H
#define AVRO_UTIL_H

#include <cstdlib>

struct avro_value;
typedef avro_value avro_value_t;


int serialize_object(avro_value_t* obj, const char** buf, size_t* len);

int deserialize_object(avro_value_t* obj, const char* buf, int64_t len);

int get_map_value(avro_value_t* schema, const char* entry_key, const char* map_name, const char** value, size_t* size);

int set_map_value(avro_value_t* schema, const char* entry_key, const char* map_name, char* value, size_t size);

#endif
