/*
 * Copyright (C) 2013 Altamira Corporation
 *
 * This file is part of Honeycomb Storage Engine.
 *
 * Honeycomb Storage Engine is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * Honeycomb Storage Engine is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Honeycomb Storage Engine.  If not, see <http://www.gnu.org/licenses/>.
 */


#ifndef JAVA_H
#define JAVA_H

class JNICache;
class Serializable;

struct JNIEnv_;
typedef JNIEnv_ JNIEnv;

class _jstring;
typedef _jstring *jstring;

class _jbyteArray;
typedef _jbyteArray *jbyteArray;

#include <stdint.h>

/**
 * @brief Prints a Java exception out to logs and stderr
 *
 * @param jni_env Java environment
 *
 * @return Whether there was an exception to print
 */
bool print_java_exception(JNIEnv* jni_env);

/**
 * @brief Check for an exception waiting in the JVM
 *
 * @param env Java environment
 * @param cache Cache of JNI classes 
 * @param location Where is this function being called from
 *
 * @return MySQL error code corresponding to the Java exception
 */
int check_exceptions(JNIEnv* env, JNICache* cache, const char* location);

/**
 * @brief Convert a C byte array into a Java byte array
 *
 * @param value C byte array
 * @param length Length of the byte array
 * @param env Java environment
 *
 * @return Java byte array
 */
jbyteArray convert_value_to_java_bytes(unsigned char* value, uint32_t length, JNIEnv* env);

/**
 * @brief Convert a Java byte array to a C byte array
 *
 * @param java_bytes Java byte array
 * @param env Java environment
 *
 * @return C byte array
 */
char *char_array_from_java_bytes(jbyteArray java_bytes, JNIEnv* env);

/**
 * @brief Convert a Java string into a C string
 *
 * @param env Java environment
 * @param str Java string 
 *
 * @return C string 
 */
const char* java_to_string(JNIEnv* env, jstring str);

/**
 * @brief Convert a C string into a Java string
 *
 * @param env Java environment
 * @param string C string
 *
 * @return Java string
 */
jstring string_to_java_string(JNIEnv* env, const char *string);

/**
 * @brief Convert a serializable object to a Java byte array
 *
 * @param env Java environment
 * @param serializable Serializable object
 *
 * @return Java byte array
 */
jbyteArray serialize_to_java(JNIEnv* env, Serializable& serializable);

/**
 * @brief Convert a Java byte array to serializable object
 *
 * @param env Java environment
 * @param bytes Java byte array 
 * @param serializable Serializable object
 */
void deserialize_from_java(JNIEnv* env, jbyteArray bytes, Serializable& serializable);

#endif
