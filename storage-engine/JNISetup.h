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


#ifndef JNISETUP_H
#define JNISETUP_H

struct JNIEnv_;
typedef JNIEnv_ JNIEnv;

class _jobject;
typedef _jobject *jobject;

struct JavaVM_;
typedef JavaVM_ JavaVM;

typedef int jint;

class Settings;


bool try_initialize_jvm(JavaVM** jvm, const Settings& options, jobject* factory);
void attach_thread(JavaVM *jvm, JNIEnv** env, const char* location = "unknown");
jint detach_thread(JavaVM *jvm);

#endif
