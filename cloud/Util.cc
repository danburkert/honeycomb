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
  if (tmp < 0)
  {
    time->neg = true;
    tmp = -tmp;
  }

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

char *extract_table_name_from_path(const char *path)
{
  char* namespaced_table = new char[strlen(path) - 1];
  char* ptr = (char*)strchr(path, '/');
  ptr++;
  sprintf(namespaced_table, "%s", ptr);
  char* slash = (char*)strchr(namespaced_table, '/');
  *slash = '.';
  return namespaced_table;
}

// Convert an integral type of count bytes to a little endian long
// Convert a buffer of length buff_length into an equivalent long long in long_buff
void bytes_to_long(const uchar* buff, unsigned int buff_length,
    const bool is_signed, uchar* long_buff)
{
  if (is_signed && buff[buff_length - 1] >= (uchar) 0x80)
  {
    memset(long_buff, 0xFF, sizeof(long));
  } else
  {
    memset(long_buff, 0x00, sizeof(long));
  }

  memcpy(long_buff, buff, buff_length);
}

uchar* create_key_copy(Field* index_field, const uchar* key, uint* key_len, THD* thd)
{
  int index_field_type = index_field->real_type();
  uchar* key_copy;
  switch (index_field_type)
  {
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_LONGLONG:
    case MYSQL_TYPE_ENUM:
      {
        key_copy = new uchar[sizeof(long long)];
        const bool is_signed = !is_unsigned_field(index_field);
        bytes_to_long(key, *key_len, is_signed, key_copy);
        *key_len = sizeof(long long);
        make_big_endian(key_copy, *key_len);
        break;
      }
    case MYSQL_TYPE_YEAR:
      {
        key_copy = new uchar[sizeof(long long)];
        // It comes to us as one byte, need to cast it to int and add 1900
        uint32_t int_val = (uint32_t) key[0] + 1900;

        bytes_to_long((uchar *) &int_val, sizeof(uint32_t), false, key_copy);
        *key_len = sizeof(long long);
        make_big_endian(key_copy, *key_len);
        break;
      }
    case MYSQL_TYPE_FLOAT:
      {
        double j = (double) floatGet(key);

        key_copy = new uchar[sizeof(double)];
        *key_len = sizeof(double);

        doublestore(key_copy, j);
        reverse_bytes(key_copy, *key_len);
        break;
      }
    case MYSQL_TYPE_DOUBLE:
      {
        double j = (double) floatGet(key);
        doubleget(j, key);

        key_copy = new uchar[sizeof(double)];
        *key_len = sizeof(double);

        doublestore(key_copy, j);
        reverse_bytes(key_copy, *key_len);
        break;
      }
    case MYSQL_TYPE_DECIMAL:
    case MYSQL_TYPE_NEWDECIMAL:
      {
        key_copy = new uchar[*key_len];
        memcpy(key_copy, key, *key_len);
        break;
      }
    case MYSQL_TYPE_DATE:
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_TIME:
    case MYSQL_TYPE_TIMESTAMP:
    case MYSQL_TYPE_NEWDATE:
      {
        MYSQL_TIME mysql_time;

        switch (index_field_type)
        {
          case MYSQL_TYPE_DATE:
          case MYSQL_TYPE_NEWDATE:
            if (*key_len == 3)
            {
              extract_mysql_newdate((long) uint3korr(key), &mysql_time);
            } else
            {
              extract_mysql_old_date((int32) uint4korr(key), &mysql_time);
            }
            break;
          case MYSQL_TYPE_TIMESTAMP:
            extract_mysql_timestamp((long) uint4korr(key), &mysql_time, thd);
            break;
          case MYSQL_TYPE_TIME:
            extract_mysql_time((long) sint3korr(key), &mysql_time);
            break;
          case MYSQL_TYPE_DATETIME:
            extract_mysql_datetime((ulonglong) uint8korr(key), &mysql_time);
            break;
        }

        char timeString[MAX_DATE_STRING_REP_LENGTH];
        my_TIME_to_str(&mysql_time, timeString);
        int length = strlen(timeString);
        key_copy = new uchar[length];
        memcpy(key_copy, timeString, length);
        *key_len = length;
        break;
      }
    case MYSQL_TYPE_VARCHAR:
      {
        /**
         * VARCHARs are prefixed with two bytes that represent the actual length of the value.
         * So we need to read the length into actual_length, then copy those bits to key_copy.
         * Thank you, MySQL...
         */
        uint16_t *short_len_ptr = (uint16_t *) key;
        *key_len = (uint) (*short_len_ptr);
        key += 2;
        key_copy = new uchar[*key_len];
        memcpy(key_copy, key, *key_len);
        break;
      }
    default:
      key_copy = new uchar[*key_len];
      memcpy(key_copy, key, *key_len);
      break;
  }

  return key_copy;
}
