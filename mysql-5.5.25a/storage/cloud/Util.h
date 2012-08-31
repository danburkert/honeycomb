#ifndef UTIL_H
#define UTIL_H

#define MYSQL_SERVER 1

#include "sql_class.h"
#include <jni.h>
#include <sql_class.h>
#include <tztime.h>

#include "Macros.h"
#include "m_string.h"

bool is_unsigned_field(Field *field);
jclass find_jni_class(const char* class_name, JNIEnv* env);
void print_java_exception(JNIEnv* jni_env);
void extract_mysql_newdate(long tmp, MYSQL_TIME *time);
void extract_mysql_old_date(int32 tmp, MYSQL_TIME *time);
void extract_mysql_time(long tmp, MYSQL_TIME *time);
void extract_mysql_datetime(longlong tmp, MYSQL_TIME *time);
void extract_mysql_timestamp(long tmp, MYSQL_TIME *time, THD *thd);

#endif
