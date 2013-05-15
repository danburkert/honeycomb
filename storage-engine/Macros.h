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


#ifndef MACROS_H
#define MACROS_H
#include <cstdlib>
#include "Logging.h"

#define INFO(format_string) DBUG_PRINT("INFO", format_string)
#define ERROR(format_string) DBUG_PRINT("ERROR", format_string)

#define ARRAY_DELETE(arr) do { if (arr != NULL) { delete[] arr; arr = NULL; } } while(0)
#define MY_FREE(buf) do { if (buf != NULL) { my_free(buf); buf = NULL; } } while(0)
#define DELETE_REF(env, ref) do { if (ref != NULL) { env->DeleteLocalRef(ref); } } while(0);

/**
 * @brief Check the environment for exceptions.  If there is a pending exception,
 * abort.  Relies on env being in scope.
 *
 * @param message Message to fail with
 */
#define EXCEPTION_CHECK_ABORT(message) \
  do { if (env->ExceptionCheck()) { \
    perror("Failure during JNI call: " message); \
    Logging::fatal(message); \
    env->ExceptionDescribe(); \
    perror("Failure during JNI call. Check honeycomb.log for details."); \
    abort(); \
  }} while(0)

/**
 * @brief Check the value is not NULL, otherwise abort and log message.
 * Relies on env being in scope.
 *
 * @param value Value to check against null
 * @param message Failure message
 */
#define NULL_CHECK_ABORT(value, message) \
  do { if (value == NULL) { \
    perror("Failure during JNI call: " message); \
    Logging::fatal(message); \
    env->ExceptionDescribe(); \
    abort(); \
  }} while(0)

#define HONEYCOMB "com/nearinfinity/honeycomb/"
#define HBASECLIENT "com/nearinfinity/honeycomb/hbaseclient/"
#define MYSQLENGINE "com/nearinfinity/honeycomb/mysqlengine/"
#endif
