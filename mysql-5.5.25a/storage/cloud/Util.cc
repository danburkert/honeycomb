#include "Util.h"

void print_java_exception(JNIEnv* env)
{
  if(env->ExceptionCheck() == JNI_TRUE)
  {
    jthrowable throwable = env->ExceptionOccurred();
    jclass objClazz = env->GetObjectClass(throwable);
    jmethodID methodId = env->GetMethodID(objClazz, "toString", "()Ljava/lang/String;");
    jstring result = (jstring)env->CallObjectMethod(throwable, methodId);
    const char* string = env->GetStringUTFChars(result, NULL);
    INFO(("Exception from java: %s", string));
    env->ReleaseStringUTFChars(result, string);
  }
}

jclass find_jni_class(const char* class_name, JNIEnv* env)
{
  char buffer[1024];
  const char* path = JNI_CLASSPATH;
  sprintf(buffer, "%s%s", path, class_name);
  return env->FindClass(buffer);
}

bool is_unsigned_field(Field *field)
{
  ha_base_keytype keyType = field->key_type();
  return (keyType == HA_KEYTYPE_BINARY
       || keyType == HA_KEYTYPE_USHORT_INT
       || keyType == HA_KEYTYPE_UINT24
       || keyType == HA_KEYTYPE_ULONG_INT
       || keyType == HA_KEYTYPE_ULONGLONG);
}

hbase_data_type extract_field_type(Field *field)
{
  int fieldType = field->type();
  hbase_data_type essentialType;

  if (fieldType == MYSQL_TYPE_LONG
          || fieldType == MYSQL_TYPE_SHORT
          || fieldType == MYSQL_TYPE_TINY
          || fieldType == MYSQL_TYPE_LONGLONG
          || fieldType == MYSQL_TYPE_INT24
          || fieldType == MYSQL_TYPE_YEAR)
  {
    if (is_unsigned_field(field))
    {
      essentialType = JAVA_ULONG;
    } else {
      essentialType= JAVA_LONG;
    }
  }
  else if (fieldType == MYSQL_TYPE_DOUBLE
             || fieldType == MYSQL_TYPE_FLOAT
             || fieldType == MYSQL_TYPE_DECIMAL
             || fieldType == MYSQL_TYPE_NEWDECIMAL)
  {
    essentialType = JAVA_DOUBLE;
  }
  else if (fieldType == MYSQL_TYPE_DATE
      || fieldType == MYSQL_TYPE_NEWDATE)
  {
    essentialType = JAVA_DATE;
  }
  else if (fieldType == MYSQL_TYPE_TIME)
  {
    essentialType = JAVA_TIME;
  }
  else if (fieldType == MYSQL_TYPE_DATETIME
      || fieldType == MYSQL_TYPE_TIMESTAMP)
  {
    essentialType = JAVA_DATETIME;
  }
  else if (fieldType == MYSQL_TYPE_VARCHAR
            || fieldType == MYSQL_TYPE_ENUM)
  {
    essentialType = JAVA_STRING;
  }
  else if (fieldType == MYSQL_TYPE_BLOB
            || fieldType == MYSQL_TYPE_STRING
            || fieldType == MYSQL_TYPE_VAR_STRING
            || fieldType == MYSQL_TYPE_TINY_BLOB
            || fieldType == MYSQL_TYPE_MEDIUM_BLOB
            || fieldType == MYSQL_TYPE_LONG_BLOB)
  {
    essentialType = JAVA_BINARY;
  }
  else
  {
    essentialType = UNKNOWN_TYPE;
  }

  return essentialType;
}

enum enum_mysql_timestamp_type timestamp_type_of_mysql_type(enum enum_field_types t)
{
  switch(t)
  {
    case MYSQL_TYPE_DATE:
    case MYSQL_TYPE_NEWDATE:
      return MYSQL_TIMESTAMP_DATE;
      break;
    case MYSQL_TYPE_TIME:
      return MYSQL_TIMESTAMP_TIME;
      break;
    case MYSQL_TYPE_DATETIME:
      return MYSQL_TIMESTAMP_DATETIME;
      break;
    default:
      return MYSQL_TIMESTAMP_NONE;
      break;
  }

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

