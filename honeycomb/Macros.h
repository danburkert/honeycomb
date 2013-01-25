#ifndef MACROS_H
#define MACROS_H

#define INFO(format_string) DBUG_PRINT("INFO", format_string)
#define ERROR(format_string) DBUG_PRINT("ERROR", format_string)

#define ARRAY_DELETE(arr) do { if (arr != NULL) { delete[] arr; arr = NULL; } } while(0)
#define MY_FREE(buf) do { if (buf != NULL) { my_free(buf); buf = NULL; } } while(0)
#define DELETE_REF(env, ref) env->DeleteLocalRef(ref)

// Check the result of a JNI call is JNI_OK, otherwise abort and log message.
#define CHECK_JNI_ABORT(result, message) \
  do { if (result != JNI_OK) { \
    Logging::fatal(message); \
    perror("Failure during JNI call. Check honeycomb.log for details."); \
    abort(); \
  }} while(0)

// Check the result of a JNI call is not NULL, otherwise abort and log message.
#define CHECK_JNI_NULL_ABORT(result, message) \
  do { if (result == NULL) { \
    Logging::fatal(message); \
    perror("Failure during JNI call. Check honeycomb.log for details."); \
    abort(); \
  }} while(0)

#define HBASECLIENT "com/nearinfinity/honeycomb/hbaseclient/"
#define MYSQLENGINE "com/nearinfinity/honeycomb/mysqlengine/"
#endif
