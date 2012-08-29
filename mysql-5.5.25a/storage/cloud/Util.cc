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
  return (keyType == HA_KEYTYPE_USHORT_INT
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
            || fieldType == MYSQL_TYPE_STRING
            || fieldType == MYSQL_TYPE_VAR_STRING
            || fieldType == MYSQL_TYPE_BLOB
            || fieldType == MYSQL_TYPE_TINY_BLOB
            || fieldType == MYSQL_TYPE_MEDIUM_BLOB
            || fieldType == MYSQL_TYPE_LONG_BLOB
            || fieldType == MYSQL_TYPE_ENUM)
  {
    essentialType = JAVA_STRING;
  }
  else
  {
    essentialType = UNKNOWN_TYPE;
  }

  return essentialType;
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
  time->year = (int) ((int32) tmp / 10000L % 10000);
  time->month = (int) ((int32) tmp / 100 % 100);
  time->day = (int) ((int32) tmp % 100);
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

void extract_mysql_datetime(ulonglong tmp, MYSQL_TIME *time)
{
  bzero((void*) time, sizeof(*time));
  long part1, part2;
  part1 = (long) (tmp / LL(1000000));
  part2 = (long) (tmp - (ulonglong)part1 * LL(1000000));

  time->month = part1 & 31;
  time->day = part1 >> 5 & 15;
  time->year = part1 >> 9;

  time->hour = (uint) (part2 / 10000);
  time->minute = (uint) (part2 / 100 % 100);
  time->second = (uint) (part2 % 100);

  time->time_type = MYSQL_TIMESTAMP_DATETIME;
}
