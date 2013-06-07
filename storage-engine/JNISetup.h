/*
 * Copyright (C) 2013 Near Infinity Corporation
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


/**
 * @brief Try to create and initialize a new JVM based on the settings passed in.
 * The settings are passed into the JVM as command line arguments. 
 * The function also creates a factory for instantiating new HandlerProxies with 
 * all dependencies satisfied.
 * If a JVM already exists then this function will fail.
 *
 * @param jvm Java VM that will be created and setup 
 * @param options Settings for the Java VM (will be passed into the command line)
 * @param factory Factory that creates the HandlerProxy class (will be created)
 *
 * @return Was initialization successful?
 */
bool try_initialize_jvm(JavaVM** jvm, const Settings& options, jobject* factory);

/**
 * @brief Attach current thread to the JVM. Assign current environment to env. Keeps
 * track of how often the current thread has attached, and will not detach
 * until the number of calls to detach is the same as the number of calls to
 * attach.  If attach fails, does its best to log the error and aborts.  Not
 * being able to attach to the JNI is an unrecoverable error.
 *
 * @param jvm Java VM
 * @param env Java environment to attach the thread
 * @param location Who is calling the function
 */
void attach_thread(JavaVM *jvm, JNIEnv** env, const char* location = "unknown");

/**
 * @brief Detach thread from JVM. Will not detach unless the number of calls to
 * detach is the same as the number of calls to attach.
 *
 * @param jvm Java VM
 *
 * @return JNI_OK if successful, or a negative number on failure.
 */
jint detach_thread(JavaVM *jvm);

#endif
