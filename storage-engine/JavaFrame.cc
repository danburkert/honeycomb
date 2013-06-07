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


#include "JavaFrame.h"
#include "Logging.h"
#include "Macros.h"
#include <jni.h>

/**
 * Create JNI Stack Frame.  If enough memory is not able to be allocated, log
 * errors and abort.
 */
JavaFrame::JavaFrame(JNIEnv* env, int capacity) : env(env)
{
  if (env->PushLocalFrame(capacity))
  {
    const char* msg = "Unable to push local frame.  Out of memory. Aborting.";
    perror(msg);
    Logging::fatal(msg);
    abort();
  }
}

JavaFrame::~JavaFrame()
{
  env->PopLocalFrame(NULL);
}
