#ifndef JAVA_H
#define JAVA_H

#include <jni.h>
#include "my_global.h"
#include "my_base.h"
#include "JNICache.h"

jfieldID find_flag_to_java(enum ha_rkey_function find_flag, JNICache* cache);
bool print_java_exception(JNIEnv* jni_env);
jbyteArray convert_value_to_java_bytes(uchar* value, uint32 length, JNIEnv* env);
char *char_array_from_java_bytes(jbyteArray java_bytes, JNIEnv* env);
int check_exceptions(JNIEnv* env, JNICache* cache, const char* location);

#endif
