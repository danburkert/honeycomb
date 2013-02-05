#ifndef UTIL_H
#define UTIL_H

#ifndef MYSQL_SERVER
#define MYSQL_SERVER 1
#endif

#include "sql_class.h"
#include <stdint.h>

bool is_unsigned_field(Field *field);
void reverse_bytes(uchar *begin, uint length);
bool is_little_endian();
void make_big_endian(uchar *begin, uint length);
char *extract_table_name_from_path(const char *path);
uchar* create_key_copy(Field* index_field, const uchar* key, uint* key_len, THD* thd);
void extract_mysql_time(long tmp, MYSQL_TIME *time);
uint64_t bswap64(uint64_t x);
int count_fields(TABLE* table);
#endif
