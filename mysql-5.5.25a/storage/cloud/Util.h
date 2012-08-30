#ifndef UTIL_H
#define UTIL_H

#define MYSQL_SERVER 1

#include "sql_class.h"
#include <jni.h>
#include <sql_class.h>
#include <tztime.h>

#include "Macros.h"
#include "m_string.h"

enum hbase_data_type { UNKNOWN_TYPE, JAVA_STRING, JAVA_LONG, JAVA_ULONG, JAVA_DOUBLE, JAVA_TIME, JAVA_DATE, JAVA_DATETIME };

hbase_data_type extract_field_type(Field *field);
bool is_unsigned_field(Field *field);
jclass find_jni_class(const char* class_name, JNIEnv* env);
void print_java_exception(JNIEnv* jni_env);
void extract_mysql_newdate(long tmp, MYSQL_TIME *time);
void extract_mysql_old_date(int32 tmp, MYSQL_TIME *time);
void extract_mysql_time(long tmp, MYSQL_TIME *time);
void extract_mysql_datetime(longlong tmp, MYSQL_TIME *time);
void extract_mysql_timestamp(long tmp, MYSQL_TIME *time, THD *thd);
enum enum_mysql_timestamp_type timestamp_type_of_mysql_type(enum enum_field_types t);

#endif
