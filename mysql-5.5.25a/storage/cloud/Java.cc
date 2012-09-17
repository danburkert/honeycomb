#include "Java.h"

#define JNI_CLASSPATH "com/nearinfinity/mysqlengine/jni/"
#define MAP_CLASS "java/util/TreeMap"

jobject create_java_map(JNIEnv* env)
{
  jclass map_class = env->FindClass(MAP_CLASS);
  jmethodID constructor = env->GetMethodID(map_class, "<init>", "()V");
  return env->NewObject(map_class, constructor);
}

jobject java_map_insert(jobject java_map, jobject key, jobject value, JNIEnv* env)
{
  jclass map_class = env->FindClass(MAP_CLASS);
  jmethodID put_method = env->GetMethodID(map_class, "put", "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");

  return env->CallObjectMethod(java_map, put_method, key, value);
}

jbyteArray java_map_get(jobject java_map, jstring key, JNIEnv* env)
{
  jclass map_class = env->FindClass(MAP_CLASS);
  jmethodID get_method = env->GetMethodID(map_class, "get", "(Ljava/lang/Object;)Ljava/lang/Object;");

  return (jbyteArray) env->CallObjectMethod(java_map, get_method, key);
}

jboolean java_map_is_empty(jobject java_map, JNIEnv* env)
{
  jclass map_class = env->FindClass(MAP_CLASS);
  jmethodID is_empty_method = env->GetMethodID(map_class, "isEmpty", "()Z");
  jboolean result = env->CallBooleanMethod(java_map, is_empty_method);
  return (bool) result;
}

jobject find_flag_to_java(enum ha_rkey_function find_flag, JNIEnv* env)
{
  const char* index_type_path = "Lcom/nearinfinity/mysqlengine/jni/IndexReadType;";
  jclass read_class = find_jni_class("IndexReadType", env);
  jfieldID field_id;
  if (find_flag == HA_READ_KEY_EXACT)
  {
    field_id = env->GetStaticFieldID(read_class, "HA_READ_KEY_EXACT", index_type_path);
  }
  else if(find_flag == HA_READ_AFTER_KEY)
  {
    field_id = env->GetStaticFieldID(read_class, "HA_READ_AFTER_KEY", index_type_path);
  }
  else if(find_flag == HA_READ_KEY_OR_NEXT)
  {
    field_id = env->GetStaticFieldID(read_class, "HA_READ_KEY_OR_NEXT", index_type_path);
  }
  else if(find_flag == HA_READ_KEY_OR_PREV)
  {
    field_id = env->GetStaticFieldID(read_class, "HA_READ_KEY_OR_PREV", index_type_path);
  }
  else if(find_flag == HA_READ_BEFORE_KEY)
  {
    field_id = env->GetStaticFieldID(read_class, "HA_READ_BEFORE_KEY", index_type_path);
  }
  else
  {
    return NULL;
  }

  return env->GetStaticObjectField(read_class, field_id);
}

jobject java_find_flag_by_name(const char *name, JNIEnv* env)
{
  jclass read_class = find_jni_class("IndexReadType", env);
  jfieldID field_id = env->GetStaticFieldID(read_class, name, "Lcom/nearinfinity/mysqlengine/jni/IndexReadType;");
  return env->GetStaticObjectField(read_class, field_id);
}

void print_java_exception(JNIEnv* env)
{
  if(env->ExceptionCheck() == JNI_TRUE)
  {
    jthrowable throwable = env->ExceptionOccurred();
    jclass objClazz = env->GetObjectClass(throwable);
    jmethodID methodId = env->GetMethodID(objClazz, "toString", "()Ljava/lang/String;");
    jstring result = (jstring)env->CallObjectMethod(throwable, methodId);
    const char* string = env->GetStringUTFChars(result, NULL);
    Logging::info("Exception from java: %s", string);
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
