#include "Util.h"

bool is_unsigned_field(Field *field)
{
  ha_base_keytype keyType = field->key_type();
  return (keyType == HA_KEYTYPE_BINARY
       || keyType == HA_KEYTYPE_USHORT_INT
       || keyType == HA_KEYTYPE_UINT24
       || keyType == HA_KEYTYPE_ULONG_INT
       || keyType == HA_KEYTYPE_ULONGLONG);
}

void extract_mysql_newdate(long tmp, MYSQL_TIME *time)
{
  bzero((void*) time, sizeof(*time));
  time->month = tmp >> 5 & 15;
  time->day = tmp & 31;
  time->year = tmp >> 9;
  time->time_type = MYSQL_TIMESTAMP_DATE;
}

void extract_mysql_old_date(int32 tmp, MYSQL_TIME *time)
{
  bzero((void*) time, sizeof(*time));
  time->year = (int) ((uint32) tmp / 10000L % 10000);
  time->month = (int) ((uint32) tmp / 100 % 100);
  time->day = (int) ((uint32) tmp % 100);
  time->time_type = MYSQL_TIMESTAMP_DATE;
}

void extract_mysql_time(long tmp, MYSQL_TIME *time)
{
  bzero((void*) time, sizeof(*time));
  time->hour = (uint) (tmp / 10000);
  time->minute = (uint) (tmp / 100 % 100);
  time->second = (uint) (tmp % 100);
  time->time_type = MYSQL_TIMESTAMP_TIME;
}

void extract_mysql_datetime(longlong tmp, MYSQL_TIME *time)
{
  bzero((void*) time, sizeof(*time));
  uint32 part1,part2;
  part1=(uint32) (tmp/LL(1000000));
  part2=(uint32) (tmp - (ulonglong) part1*LL(1000000));

  time->neg=   0;
  time->second_part= 0;
  time->second=  (int) (part2%100);
  time->minute=  (int) (part2/100%100);
  time->hour=    (int) (part2/10000);
  time->day=   (int) (part1%100);
  time->month=   (int) (part1/100%100);
  time->year=    (int) (part1/10000);
  time->time_type = MYSQL_TIMESTAMP_DATETIME;
}

void extract_mysql_timestamp(long tmp, MYSQL_TIME *time, THD *thd)
{
  bzero((void*) time, sizeof(*time));
  thd->variables.time_zone->gmt_sec_to_TIME(time, (my_time_t)tmp);
}

void reverse_bytes(uchar *begin, uint length)
{
  for(int x = 0, y = length - 1; x < y; x++, y--)
  {
    uchar tmp = begin[x];
    begin[x] = begin[y];
    begin[y] = tmp;
  }
}

bool is_little_endian()
{
#ifdef WORDS_BIG_ENDIAN
  return false;
#else
  return true;
#endif
}

float floatGet(const uchar *ptr)
{
  float j;
#ifdef WORDS_BIGENDIAN
  if (table->s->db_low_byte_first)
  {
    float4get(j,ptr);
  }
  else
#endif
    memcpy(&j, ptr, sizeof(j));

  return j;
}

void make_big_endian(uchar *begin, uint length)
{
  if (is_little_endian())
  {
    reverse_bytes(begin, length);
  }
}

const char *extract_table_name_from_path(const char *path)
{
  const char* ptr = strrchr(path, '/');
  return ptr + 1;
}
