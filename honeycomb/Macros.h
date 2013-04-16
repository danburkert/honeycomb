#ifndef MACROS_H
#define MACROS_H
#include <cstdlib>
#include "Logging.h"

#define INFO(format_string) DBUG_PRINT("INFO", format_string)
#define ERROR(format_string) DBUG_PRINT("ERROR", format_string)

#define ARRAY_DELETE(arr) do { if (arr != NULL) { delete[] arr; arr = NULL; } } while(0)
#define MY_FREE(buf) do { if (buf != NULL) { my_free(buf); buf = NULL; } } while(0)
#define DELETE_REF(env, ref) env->DeleteLocalRef(ref)

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
#define SETTINGS_BASE "/usr/local/etc/"
#endif
