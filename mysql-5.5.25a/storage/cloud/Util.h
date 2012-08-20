#ifndef UTIL_H
#define UTIL_H
#include "sql_class.h"

enum hbase_data_type { UNKNOWN_TYPE, JAVA_STRING, JAVA_LONG, JAVA_DOUBLE, JAVA_TIME, JAVA_DATE, JAVA_DATETIME };

hbase_data_type extract_field_type(Field *field);

#endif
