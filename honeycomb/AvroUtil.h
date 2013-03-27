#ifndef AVRO_UTIL_H
#define AVRO_UTIL_H

#include <avro.h>
#include <stdlib.h>

int serialize_object(avro_value_t* obj, const char** buf, size_t* len);

int deserialize_object(avro_value_t* obj, const char* buf, int64_t len);

int get_map_value(avro_value_t* schema, const char* entry_key, const char* map_name, const char** value, size_t* size);

int set_map_value(avro_value_t* schema, const char* entry_key, const char* map_name, char* value, size_t size);
#endif
