#ifndef JAVA_H
#define JAVA_H

#include <jni.h>
#include "sql_class.h"

#include "Logging.h"

jobject create_java_map(JNIEnv* env);
jobject java_map_insert(jobject java_map, jobject key, jobject value, JNIEnv* env);
jbyteArray java_map_get(jobject java_map, jstring key, JNIEnv* env);
jboolean java_map_is_empty(jobject java_map, JNIEnv* env);

jobject create_java_list(JNIEnv* env);
jobject java_list_insert(jobject java_list, jobject value, JNIEnv* env);

jobject find_flag_to_java(enum ha_rkey_function find_flag, JNIEnv* env);
jobject java_find_flag_by_name(const char *name, JNIEnv* env);
jclass find_jni_class(const char* class_name, JNIEnv* env);
bool print_java_exception(JNIEnv* jni_env);
jobject create_java_boolean(jboolean boolean, JNIEnv* env);

#endif
