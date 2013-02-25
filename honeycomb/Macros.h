#ifndef MACROS_H
#define MACROS_H
#include <cstdlib>

#define INFO(format_string) DBUG_PRINT("INFO", format_string)
#define ERROR(format_string) DBUG_PRINT("ERROR", format_string)

#define ARRAY_DELETE(arr) do { if (arr != NULL) { delete[] arr; arr = NULL; } } while(0)
#define MY_FREE(buf) do { if (buf != NULL) { my_free(buf); buf = NULL; } } while(0)
#define DELETE_REF(env, ref) env->DeleteLocalRef(ref)

/**
 * @brief Check the result of a JNI call is JNI_OK, otherwise abort and log message.
 * Relies on env being in scope.
 *
 * @param result Result of JNI call
 * @param message Failure message
 */
#define CHECK_JNI_ABORT(result, message) \
  do { if (result != JNI_OK) { \
    perror("Failure during JNI call: " message); \
    Logging::fatal(message); \
    env->ExceptionDescribe(); \
    abort(); \
  }} while(0)

/**
 * @brief Check the environment for exceptions.  If there is a pending exception,
 * write location and reason to the logger, and return HA_ERR_INTERNAL_ERROR.
 * Relies on env being in scope.
 *
 * @param location Name of surrounding function
 * @param reason Previous call into JNI
 *
 * @return Internal error
 */
#define EXCEPTION_CHECK_IE(location, reason) \
  do { if (env->ExceptionCheck()) { \
    perror("Failure during JNI call in " location ": " reason); \
    Logging::error(location ": pending java exception after " reason); \
    env->ExceptionDescribe(); \
    return HA_ERR_INTERNAL_ERROR; \
  }} while(0)

/**
 * @brief Check the environment for exceptions.  If there is a pending exception,
 * write location and reason to the logger, and DBUG_RETURN HA_ERR_INTERNAL_ERROR.
 * Relies on env being in scope.
 *
 * @param location Name of surrounding function
 * @param reason Previous call into JNI
 *
 * @return Internal error
 */
#define EXCEPTION_CHECK_DBUG_IE(location, reason) \
  do { if (env->ExceptionCheck()) { \
    perror("Failure during JNI call in " location ": " reason); \
    Logging::error(location ": pending java exception after " reason); \
    env->ExceptionDescribe(); \
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR); \
  }} while(0)

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
 * @brief Check the environment for exceptions.  If there is a pending exception,
 * write location and reason to the logger.  Relies on env being in scope.
 *
 * @param location Name of surrounding function
 * @param reason Reason for failure
 */
#define EXCEPTION_CHECK(location, reason) \
  do { if (env->ExceptionCheck()) { \
    perror("Failure during JNI call in " location ": " reason); \
    Logging::error(location ": pending java exception after " reason); \
    env->ExceptionDescribe(); \
  }} while(0)

/**
 * @brief Check the value against null.  If null, write location and reason to the logger,
 * and return HA_ERR_INTERNAL_ERROR.  Relies on env being in scope.
 *
 * @param value To check against null
 * @param location Name of surrounding function
 * @param reason Reason for failure
 */
#define NULL_CHECK_IE(value, location, reason) \
  do { if (value == NULL) { \
    perror("Failure during JNI call in " location ": " reason); \
    Logging::error(location ": pending java exception after " reason); \
    env->ExceptionDescribe(); \
    return HA_ERR_INTERNAL_ERROR; \
  }} while(0)

/**
 * @brief Check the value is not NULL, otherwise abort and log message.
 * Relies on env being in scope.
 *
 * @param value Value to check against null
 * @param message Failure message
 *
 * @return 
 */
#define NULL_CHECK_ABORT(value, message) \
  do { if (value == NULL) { \
    perror("Failure during JNI call: " message); \
    Logging::fatal(message); \
    env->ExceptionDescribe(); \
    abort(); \
  }} while(0)

#define HBASECLIENT "com/nearinfinity/honeycomb/hbaseclient/"
#define MYSQLENGINE "com/nearinfinity/honeycomb/mysqlengine/"
#endif
