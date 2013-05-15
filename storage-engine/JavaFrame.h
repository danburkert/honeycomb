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


#ifndef JAVAFRAME_H
#define JAVAFRAME_H

struct JNIEnv_;
typedef JNIEnv_ JNIEnv;

/**
  * @brief Creates a frame for tracking local references for JNI objects. The destructors frees JNI objects on scope exit.
 *
 * Pretty much any call into JNIEnv that return a non-primitive (i.e. inherits from jobject) is a local reference.
 * Java provides a set of functions for explicitly managing the lifetime
 * of JNI local references instead of waiting for the JVM to cleanup the objects. PushLocalFrame and PopLocalFrame are
 * used to save a frame of the allocated JNI objects.
 */
class JavaFrame
{
  private:
    JNIEnv* env;
  public:
    JavaFrame(JNIEnv* env, int capacity = 10);
    ~JavaFrame();
};

#endif
