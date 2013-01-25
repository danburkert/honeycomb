#include "Java.h"
#include "Macros.h"
#include "Logging.h"
#include <string.h>

#define MAP_CLASS "java/util/TreeMap"
#define LIST_CLASS "java/util/LinkedList"

jboolean java_map_is_empty(jobject java_map, JNIEnv* env)
{
  jclass map_class = env->FindClass(MAP_CLASS);
  jmethodID is_empty_method = env->GetMethodID(map_class, "isEmpty", "()Z");
  jboolean result = env->CallBooleanMethod(java_map, is_empty_method);
  return (bool) result;
}

jfieldID find_flag_to_java(enum ha_rkey_function find_flag, JNICache* cache)
{
  if (find_flag == HA_READ_KEY_EXACT)
  {
    return cache->index_read_type().read_key_exact;
  }
  else if(find_flag == HA_READ_AFTER_KEY)
  {
    return cache->index_read_type().read_after_key;
  }
  else if(find_flag == HA_READ_KEY_OR_NEXT)
  {
    return cache->index_read_type().read_key_or_next;
  }
  else if(find_flag == HA_READ_KEY_OR_PREV)
  {
    return cache->index_read_type().read_key_or_prev;
  }
  else if(find_flag == HA_READ_BEFORE_KEY)
  {
    return cache->index_read_type().read_before_key;
  }
  else
  {
    return NULL;
  }
}

bool print_java_exception(JNIEnv* env)
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
    return true;
  }

  return false;
}

jbyteArray convert_value_to_java_bytes(uchar* value, uint32 length, JNIEnv* env)
{
  jbyteArray byteArray = env->NewByteArray(length);
  jbyte *java_bytes = env->GetByteArrayElements(byteArray, 0);

  memcpy(java_bytes, value, length);

  env->SetByteArrayRegion(byteArray, 0, length, java_bytes);
  env->ReleaseByteArrayElements(byteArray, java_bytes, 0);

  return byteArray;
}

char *char_array_from_java_bytes(jbyteArray java_bytes, JNIEnv* env)
{
  int length = (int) env->GetArrayLength(java_bytes);
  jbyte *jbytes = env->GetByteArrayElements(java_bytes, JNI_FALSE);

  char* ret = new char[length];

  memcpy(ret, jbytes, length);

  env->ReleaseByteArrayElements(java_bytes, jbytes, 0);

  return ret;
}
