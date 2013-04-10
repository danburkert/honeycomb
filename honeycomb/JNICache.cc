#include "JNICache.h"
#include "JNISetup.h"
#include "JavaFrame.h"
#include "Logging.h"
#include "Macros.h"
JNICache::JNICache(JavaVM* jvm) : jvm(jvm)
{
  JNIEnv* env;
  attach_thread(jvm, &env, "JNICache::JNICache");

  // (dburkert:) I do not recommend editing this section without javap -s,
  // editor macros, and tabular.vim

  handler_proxy_.clazz                    = get_class_ref(env, HONEYCOMB "mysql/HandlerProxy");
  handler_proxy_.create_table             = get_method_id(env, handler_proxy_.clazz, "createTable", "(Ljava/lang/String;[BJ)V");
  handler_proxy_.drop_table               = get_method_id(env, handler_proxy_.clazz, "dropTable", "(Ljava/lang/String;)V");
  handler_proxy_.rename_table             = get_method_id(env, handler_proxy_.clazz, "renameTable", "(Ljava/lang/String;Ljava/lang/String;)V");
  handler_proxy_.open_table               = get_method_id(env, handler_proxy_.clazz, "openTable", "(Ljava/lang/String;)V");
  handler_proxy_.close_table              = get_method_id(env, handler_proxy_.clazz, "closeTable", "()V");
  handler_proxy_.get_row_count            = get_method_id(env, handler_proxy_.clazz, "getRowCount", "()J");
  handler_proxy_.get_row                  = get_method_id(env, handler_proxy_.clazz, "getRow", "([B)[B");
  handler_proxy_.start_index_scan         = get_method_id(env, handler_proxy_.clazz, "startIndexScan", "([B)V");
  handler_proxy_.start_table_scan         = get_method_id(env, handler_proxy_.clazz, "startTableScan", "()V");
  handler_proxy_.end_scan                 = get_method_id(env, handler_proxy_.clazz, "endScan", "()V");
  handler_proxy_.get_next_row             = get_method_id(env, handler_proxy_.clazz, "getNextRow", "()[B");
  handler_proxy_.flush                    = get_method_id(env, handler_proxy_.clazz, "flush", "()V");
  handler_proxy_.add_index                = get_method_id(env, handler_proxy_.clazz, "addIndex", "(Ljava/lang/String;[B)V");
  handler_proxy_.drop_index               = get_method_id(env, handler_proxy_.clazz, "dropIndex", "(Ljava/lang/String;)V");
  handler_proxy_.index_contains_duplicate = get_method_id(env, handler_proxy_.clazz, "indexContainsDuplicate", "(Ljava/lang/String;[B)Z");
  handler_proxy_.insert_row               = get_method_id(env, handler_proxy_.clazz, "insertRow", "([B)V");
  handler_proxy_.update_row               = get_method_id(env, handler_proxy_.clazz, "updateRow", "([B)V");
  handler_proxy_.delete_row               = get_method_id(env, handler_proxy_.clazz, "deleteRow", "([B)V");
  handler_proxy_.delete_all_rows          = get_method_id(env, handler_proxy_.clazz, "deleteAllRows", "()V");
  handler_proxy_.truncate_table           = get_method_id(env, handler_proxy_.clazz, "truncateTable", "()V");
  handler_proxy_.increment_row_count      = get_method_id(env, handler_proxy_.clazz, "incrementRowCount", "(J)V");
  handler_proxy_.increment_auto_increment = get_method_id(env, handler_proxy_.clazz, "incrementAutoIncrement", "(J)J");
  handler_proxy_.get_auto_increment       = get_method_id(env, handler_proxy_.clazz, "getAutoIncrement", "()J");
  handler_proxy_.set_auto_increment       = get_method_id(env, handler_proxy_.clazz, "setAutoIncrement", "(J)V");

  TableExistsException   = get_class_ref(env, HONEYCOMB "exceptions/TableExistsException");
  TableNotFoundException = get_class_ref(env, HONEYCOMB "exceptions/TableNotFoundException");
  RowNotFoundException   = get_class_ref(env, HONEYCOMB "exceptions/RowNotFoundException");
  StoreNotFoundException = get_class_ref(env, HONEYCOMB "exceptions/StoreNotFoundException");
  RuntimeIOException     = get_class_ref(env, HONEYCOMB "exceptions/RuntimeIOException");

  throwable_.clazz             = get_class_ref(env, "java/lang/Throwable");
  throwable_.print_stack_trace = get_method_id(env, throwable_.clazz, "printStackTrace", "(Ljava/io/PrintWriter;)V");

  print_writer_.clazz = get_class_ref(env, "java/io/PrintWriter");
  print_writer_.init  = get_method_id(env, print_writer_.clazz, "<init>", "(Ljava/io/Writer;)V");

  string_writer_.clazz     = get_class_ref(env, "java/io/StringWriter");
  string_writer_.init      = get_method_id(env, string_writer_.clazz, "<init>", "()V");
  string_writer_.to_string = get_method_id(env, string_writer_.clazz, "toString", "()Ljava/lang/String;");

  handler_proxy_factory_.clazz              = get_class_ref(env, "com/nearinfinity/honeycomb/mysql/HandlerProxyFactory");
  handler_proxy_factory_.createHandlerProxy = get_method_id(env, handler_proxy_factory_.clazz, "createHandlerProxy", "()Lcom/nearinfinity/honeycomb/mysql/HandlerProxy;");

  detach_thread(jvm);
}

JNICache::~JNICache()
{
  // Setup env
  JNIEnv* env;
  attach_thread(jvm, &env, "JNICache::~JNICache");

  env->DeleteGlobalRef(handler_proxy_.clazz);
  env->DeleteGlobalRef(handler_proxy_factory_.clazz);
  env->DeleteGlobalRef(throwable_.clazz);
  env->DeleteGlobalRef(print_writer_.clazz);
  env->DeleteGlobalRef(string_writer_.clazz);

  env->DeleteGlobalRef(TableNotFoundException);
  env->DeleteGlobalRef(TableExistsException);
  env->DeleteGlobalRef(RowNotFoundException);
  env->DeleteGlobalRef(RuntimeIOException);
  env->DeleteGlobalRef(StoreNotFoundException);

  detach_thread(jvm);
}

/**
 * Find class ref of clazz in env, and return a global reference to it.
 * Abort if the class is not found, or if there is not enough memory
 * to create references to it.
 */
jclass JNICache::get_class_ref(JNIEnv* env, const char* clazz)
{
  JavaFrame frame(env, 1);
  jclass local_clazz_ref = env->FindClass(clazz);
  if (local_clazz_ref == NULL)
  {
    char log_buffer[200];
    snprintf(log_buffer, sizeof(log_buffer),
        "JNICache: Failed to find class %s", clazz);
    Logging::fatal(log_buffer);
    perror("Failure during JNI class lookup. Check honeycomb.log for details.");
    env->ExceptionDescribe();
    abort();
  }
  jclass clazz_ref = (jclass) env->NewGlobalRef(local_clazz_ref);
  if (clazz_ref == NULL)
  {
    char log_buffer[200];
    snprintf(log_buffer, sizeof(log_buffer),
        "JNICache: Not enough JVM memory to create global reference to class %s", clazz);
    Logging::fatal(log_buffer);
    perror("Failure during JNI reference creation. Check honeycomb.log for details.");
    env->ExceptionDescribe();
    abort();
  }
  return clazz_ref;
}

/**
 * Find id of method with signature on class clazz in env, and return it. Abort
 * if the field is not found.
 */
jmethodID JNICache::get_method_id(JNIEnv* env, jclass clazz, const char* method, const char* signature)
{
  jmethodID method_id = env->GetMethodID(clazz, method, signature);
  if (method_id == NULL)
  {
    char log_buffer[200];
    snprintf(log_buffer, sizeof(log_buffer),
        "JNICache: Failed to find method %s with signature %s", method, signature);
    Logging::fatal(log_buffer);
    perror("Failure during JNI method id lookup. Check honeycomb.log for details.");
    abort();
  }
  return method_id;
}

/**
 * Find id of static method with signature on class clazz in env, and
 * return it. Abort if the field is not found.
 */
jmethodID JNICache::get_static_method_id(JNIEnv* env, jclass clazz, const char* method, const char* signature)
{
  jmethodID method_id = env->GetStaticMethodID(clazz, method, signature);
  if (method_id == NULL)
  {
    char log_buffer[200];
    snprintf(log_buffer, sizeof(log_buffer),
        "JNICache: Failed to find method %s with signature %s", method, signature);
    Logging::fatal(log_buffer);
    perror("Failure during JNI static method id lookup. Check honeycomb.log for details.");
    abort();
  }
  return method_id;
}

/**
 * Find id of static field with type on class clazz in env, and return it.
 * Abort if the field is not found.
 */
jfieldID JNICache::get_static_field_id(JNIEnv* env, jclass clazz, const char* field, const char* type)
{
  jfieldID field_id = env->GetStaticFieldID(clazz, field, type);
  if (field_id == NULL)
  {
    char log_buffer[200];
    snprintf(log_buffer, sizeof(log_buffer),
        "JNICache: Failed to find static field %s with type %s", field, type);
    Logging::fatal(log_buffer);
    perror("Failure during JNI static field id lookup. Check honeycomb.log for details.");
    abort();
  }
  return field_id;
}
