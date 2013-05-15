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

#include "my_global.h"

bool print_java_exception(JNIEnv* jni_env);
int check_exceptions(JNIEnv* env, JNICache* cache, const char* location);

jbyteArray convert_value_to_java_bytes(uchar* value, uint32 length, JNIEnv* env);
char *char_array_from_java_bytes(jbyteArray java_bytes, JNIEnv* env);

const char* java_to_string(JNIEnv* env, jstring str);
jstring string_to_java_string(JNIEnv* env, const char *string);

jbyteArray serialize_to_java(JNIEnv* env, Serializable& serializable);
void deserialize_from_java(JNIEnv* env, jbyteArray bytes, Serializable& serializable);

#endif
