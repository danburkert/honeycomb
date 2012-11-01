#include "Java.h"

#define JNI_CLASSPATH "com/nearinfinity/mysqlengine/jni/"
#define MAP_CLASS "java/util/TreeMap"
#define LIST_CLASS "java/util/LinkedList"

jobject create_java_list(JNIEnv* env)
{
  jclass list_class = env->FindClass(LIST_CLASS);
  jmethodID constructor = env->GetMethodID(list_class, "<init>", "()V");
  return env->NewObject(list_class, constructor);
}

jobject java_list_insert(jobject java_list, jobject value, JNIEnv* env)
{
  jclass list_class = env->FindClass(LIST_CLASS);
  jmethodID add_method = env->GetMethodID(list_class, "add", "(Ljava/lang/Object;)Z");

  return env->CallObjectMethod(java_list, add_method, value);
}

jobject create_java_boolean(jboolean boolean, JNIEnv* env)
{
  jclass bool_class = env->FindClass("java/lang/Boolean");
  jmethodID constructor = env->GetMethodID(bool_class, "<init>", "(Z)V");
  return env->NewObject(bool_class, constructor, boolean);
}

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
    env->ExceptionDescribe();
    jclass objClazz = env->FindClass("java/lang/Throwable");

    jclass stringwriter_class = env->FindClass("java/io/StringWriter");
    jmethodID strwriter_ctor = env->GetMethodID(stringwriter_class, "<init>", "()V");
    jobject str_writer = env->NewObject(stringwriter_class, strwriter_ctor);

    jclass printer_class = env->FindClass("java/io/PrintWriter");
    jmethodID printwriter_ctor = env->GetMethodID(printer_class, "<init>", "(Ljava/io/Writer;)V");
    jobject printer = env->NewObject(printer_class, printwriter_ctor, str_writer);
    jmethodID print_stack_trace = env->GetMethodID(objClazz, "printStackTrace", "(Ljava/io/PrintWriter;)V");
    env->CallVoidMethod(throwable, print_stack_trace, printer);

    jmethodID methodId = env->GetMethodID(stringwriter_class, "toString", "()Ljava/lang/String;");
    jstring result = (jstring)env->CallObjectMethod(str_writer, methodId);
    const char* string = env->GetStringUTFChars(result, NULL);
    Logging::error("Exception from java: %s", string);
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
