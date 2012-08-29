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
