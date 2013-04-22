#include "HoneycombHandler.h"
#include "Java.h"
#include "Macros.h"
#include "Logging.h"
#include "Serializable.h"
#include <string.h>

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

/**
 * Convert native byte array to java byte array.  Returns a JNI local ref to
 * the java byte array.  This reference must be deleted by the caller (or the
 * call should happen inside a JavaFrame, it uses 1 LocalRef).
 */
jbyteArray convert_value_to_java_bytes(uchar* value, uint32 length, JNIEnv* env)
{
  jbyteArray byteArray = env->NewByteArray(length);
  NULL_CHECK_ABORT(byteArray, "Java::convert_value_to_java_bytes: OutOfMemoryException while calling NewByteArray");
  env->SetByteArrayRegion(byteArray, 0, length, (jbyte*) value);
  EXCEPTION_CHECK_ABORT("Java::convert_value_to_java_bytes: Exception while calling SetByteArrayRegion");
  return byteArray;
}

/**
 * Convert java byte array to native char array.  Returns a pointer to the
 * native array, which is heap allocated and must be array deleted by the caller.
 */
char *char_array_from_java_bytes(jbyteArray java_bytes, JNIEnv* env)
{
  int length = (int) env->GetArrayLength(java_bytes);
  char* ret = new char[length];
  env->GetByteArrayRegion(java_bytes, 0, length, (jbyte*) ret);
  EXCEPTION_CHECK_ABORT("Java::char_array_from_java_bytes: ArrayIndexOutOfBoundsException while calling GetByteArrayRegion");
  return ret;
}

int check_exceptions(JNIEnv* env, JNICache* cache, const char* location)
{
  int ret = 0;
  jthrowable e = env->ExceptionOccurred();
  if (e)
  {
    if (env->IsInstanceOf(e, cache->RuntimeIOException))
    {
      ret = HA_ERR_INTERNAL_ERROR;
    } else if (env->IsInstanceOf(e, cache->UnknownSchemaVersionException))
    {
      ret = HA_ERR_INTERNAL_ERROR;
    } else if (env->IsInstanceOf(e, cache->TableNotFoundException))
    {
      ret = HA_ERR_NO_SUCH_TABLE;
    } else if (env->IsInstanceOf(e, cache->RowNotFoundException))
    {
      ret = HA_ERR_KEY_NOT_FOUND;
    } else if (env->IsInstanceOf(e, cache->StoreNotFoundException))
    {
      my_printf_error(ER_ILLEGAL_HA, "Unable to open tablespace.", MYF(0));
      ret = HA_ERR_INTERNAL_ERROR;
    } else {
      ret = HA_ERR_GENERIC;
    }

    print_java_exception(env);
    Logging::error("Exception thrown during JNI call in %s", location);
  }
  env->DeleteLocalRef(e);
  return ret;
};

/**
 * Create java string from native string.  The returned jstring is a local reference
 * which must be deleted.  Aborts if the string cannot be constructed.
 */
jstring string_to_java_string(JNIEnv* env, const char *string)
{
  jstring jstring = env->NewStringUTF(string);
  NULL_CHECK_ABORT(jstring, "HoneycombHandler::string_to_java_string: OutOfMemoryError while calling NewStringUTF");
  return jstring;
}

/**
 * Create const char* string from java string.  The passed in java string is NOT
 * cleaned up, cleaned up with a call to
 * ReleaseStringUTFChars(jstring, native_string).
 */
const char* java_to_string(JNIEnv* env, jstring string)
{
  const char* chars = env->GetStringUTFChars(string, JNI_FALSE);
  NULL_CHECK_ABORT(chars, "HoneycombHandler::java_to_string: OutOfMemoryError while calling GetStringUTFChars");
  return chars;
}

void deserialize_from_java(JNIEnv* env, jbyteArray bytes, Serializable& serializable)
{
  jbyte* buf = env->GetByteArrayElements(bytes, JNI_FALSE);
  serializable.deserialize((const char*) buf, env->GetArrayLength(bytes));
  env->ReleaseByteArrayElements(bytes, buf, 0);
}

jbyteArray serialize_to_java(JNIEnv* env, Serializable& serializable)
{
  const char* serialized_buf;
  size_t buf_len;
  serializable.serialize(&serialized_buf, &buf_len);
  jbyteArray jserialized_key = convert_value_to_java_bytes((uchar*) serialized_buf, buf_len, env);
  delete[] serialized_buf;
  return jserialized_key;
}
