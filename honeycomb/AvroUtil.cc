#include "AvroUtil.h"
#include <avro.h>

int serialize_object(avro_value_t* obj, const char** buf, size_t* len)
{
  int ret = 0;
  ret |= avro_value_sizeof(obj, len);
  *buf = new char[*len];
  if(*buf)
  {
    avro_writer_t writer = avro_writer_memory(*buf, *len);
    ret |= avro_value_write(writer, obj);
    avro_writer_free(writer);
  } else {
    ret = -1;
  }
  return ret;
}

int deserialize_object(avro_value_t* obj, const char* buf, int64_t len)
{
  int ret = 0;
  avro_reader_t reader = avro_reader_memory(buf, len);
  ret |= avro_value_read(reader, obj);
  avro_reader_free(reader);
  return ret;
}

int get_map_value(avro_value_t* schema, const char* entry_key, const char* map_name, const char** value, size_t* size)
{
  int ret = 0;
  avro_value_t map;
  avro_value_t entry;

  // Retrieve map
  ret |= avro_value_get_by_name(schema, map_name, &map, NULL);
  // Retrieve entry from map
  ret |= avro_value_get_by_name(&map, entry_key, &entry, NULL);

  if (entry.self == NULL)
  {
    *value = NULL;
  }
  else
  {
    // Retrive the entry's value
    ret |= avro_value_get_bytes(&entry, (const void**) value, size);
  }

  return ret;
}

int set_map_value(avro_value_t* schema, const char* entry_key,
    const char* map_name, char* value, size_t size)
{
  int ret = 0;
  avro_value_t entry;
  avro_value_t map;
  // Retrieve map
  ret |= avro_value_get_by_name(schema, map_name, &map, NULL);
  // Add value to map
  ret |= avro_value_add(&map, entry_key, &entry, NULL, NULL);
  // Set new entry's value
  ret |= avro_value_set_bytes(&entry, value, size);
  return ret;
}
