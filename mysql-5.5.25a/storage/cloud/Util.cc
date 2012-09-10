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

