#include "AvroUtil.h"
#include <stdio.h>

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
