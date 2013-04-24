#ifndef JAVA_H
#define JAVA_H

#include <jni.h>

class JNICache;
class Serializable;

bool print_java_exception(JNIEnv* jni_env);
int check_exceptions(JNIEnv* env, JNICache* cache, const char* location);

jbyteArray convert_value_to_java_bytes(uchar* value, uint32 length, JNIEnv* env);
char *char_array_from_java_bytes(jbyteArray java_bytes, JNIEnv* env);

const char* java_to_string(JNIEnv* env, jstring str);
jstring string_to_java_string(JNIEnv* env, const char *string);

jbyteArray serialize_to_java(JNIEnv* env, Serializable& serializable);
void deserialize_from_java(JNIEnv* env, jbyteArray bytes, Serializable& serializable);

#endif
