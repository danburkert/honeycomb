#ifndef UTIL_H
#define UTIL_H

#define MYSQL_SERVER 1

#include "sql_class.h"
#include <jni.h>
#include <tztime.h>

#include "Macros.h"
#include "m_string.h"

bool is_unsigned_field(Field *field);
void reverse_bytes(uchar *begin, uint length);
bool is_little_endian();
void make_big_endian(uchar *begin, uint length);
const char *extract_table_name_from_path(const char *path);
uchar* create_key_copy(Field* index_field, const uchar* key, uint* key_len, THD* thd);

#endif
