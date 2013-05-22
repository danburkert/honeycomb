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


#include "JNICache.h"
#include "JNISetup.h"
#include "JavaFrame.h"
#include "Logging.h"
#include "Macros.h"
#include <jni.h>

JNICache::JNICache(JavaVM* jvm)
: jvm(jvm),
  error(false),
  handler_proxy_(),
  throwable_(),
  print_writer_(),
  string_writer_(),
  handler_proxy_factory_(),
  TableNotFoundException(NULL),
  RowNotFoundException(NULL),
  StorageBackendCreationException(NULL),
  RuntimeIOException(NULL),
  UnknownSchemaVersionException(NULL)
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
  handler_proxy_.update_row               = get_method_id(env, handler_proxy_.clazz, "updateRow", "([B[B)V");
  handler_proxy_.delete_row               = get_method_id(env, handler_proxy_.clazz, "deleteRow", "([B)V");
  handler_proxy_.delete_all_rows          = get_method_id(env, handler_proxy_.clazz, "deleteAllRows", "()V");
  handler_proxy_.truncate_table           = get_method_id(env, handler_proxy_.clazz, "truncateTable", "()V");
  handler_proxy_.increment_row_count      = get_method_id(env, handler_proxy_.clazz, "incrementRowCount", "(J)V");
  handler_proxy_.increment_auto_increment = get_method_id(env, handler_proxy_.clazz, "incrementAutoIncrement", "(J)J");
  handler_proxy_.get_auto_increment       = get_method_id(env, handler_proxy_.clazz, "getAutoIncrement", "()J");
  handler_proxy_.set_auto_increment       = get_method_id(env, handler_proxy_.clazz, "setAutoIncrement", "(J)V");


  TableNotFoundException          = get_class_ref(env, HONEYCOMB "exceptions/TableNotFoundException");
  RowNotFoundException            = get_class_ref(env, HONEYCOMB "exceptions/RowNotFoundException");
  StorageBackendCreationException = get_class_ref(env, HONEYCOMB "exceptions/StorageBackendCreationException");
  RuntimeIOException              = get_class_ref(env, HONEYCOMB "exceptions/RuntimeIOException");
  UnknownSchemaVersionException   = get_class_ref(env, HONEYCOMB "exceptions/UnknownSchemaVersionException");

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

  DELETE_REF(env, handler_proxy_.clazz);
  DELETE_REF(env, handler_proxy_factory_.clazz);
  DELETE_REF(env, throwable_.clazz);
  DELETE_REF(env, print_writer_.clazz);
  DELETE_REF(env, string_writer_.clazz);

  DELETE_REF(env, TableNotFoundException);
  DELETE_REF(env, RowNotFoundException);
  DELETE_REF(env, RuntimeIOException);
  DELETE_REF(env, StorageBackendCreationException);
  DELETE_REF(env, UnknownSchemaVersionException);

  detach_thread(jvm);
}

/**
 * Find class ref of clazz in env, and return a global reference to it.
 * Set error if the class is not found, or if there is not enough memory
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
    perror("Failure during JNI class lookup. Check log file for details.");
    env->ExceptionDescribe();
    this->error = true;
    return NULL;
  }

  jclass clazz_ref = (jclass) env->NewGlobalRef(local_clazz_ref);
  if (clazz_ref == NULL)
  {
    char log_buffer[200];
    snprintf(log_buffer, sizeof(log_buffer),
        "JNICache: Not enough JVM memory to create global reference to class %s", clazz);
    Logging::fatal(log_buffer);
    perror("Failure during JNI reference creation. Check log file for details.");
    env->ExceptionDescribe();
    this->error = true;
  }
  return clazz_ref;
}

/**
 * Find id of method with signature on class clazz in env, and return it. Set error
 * if the field is not found.
 */
jmethodID JNICache::get_method_id(JNIEnv* env, jclass clazz, const char* method, const char* signature)
{
  if (clazz == NULL)
    return NULL;

  jmethodID method_id = env->GetMethodID(clazz, method, signature);
  if (method_id == NULL)
  {
    char log_buffer[200];
    snprintf(log_buffer, sizeof(log_buffer),
        "JNICache: Failed to find method %s with signature %s", method, signature);
    Logging::fatal(log_buffer);
    perror("Failure during JNI method id lookup. Check log file for details.");
    this->error = true;
  }
  return method_id;
}

/**
 * Find id of static method with signature on class clazz in env, and
 * return it. Set error if the field is not found.
 */
jmethodID JNICache::get_static_method_id(JNIEnv* env, jclass clazz, const char* method, const char* signature)
{
  if (clazz == NULL)
    return NULL;
  jmethodID method_id = env->GetStaticMethodID(clazz, method, signature);
  if (method_id == NULL)
  {
    char log_buffer[200];
    snprintf(log_buffer, sizeof(log_buffer),
        "JNICache: Failed to find method %s with signature %s", method, signature);
    Logging::fatal(log_buffer);
    perror("Failure during JNI static method id lookup. Check log file for details.");
    this->error = true;
  }
  return method_id;
}

/**
 * Find id of static field with type on class clazz in env, and return it.
 * Set error if the field is not found.
 */
jfieldID JNICache::get_static_field_id(JNIEnv* env, jclass clazz, const char* field, const char* type)
{
  if (clazz == NULL)
    return NULL;

  jfieldID field_id = env->GetStaticFieldID(clazz, field, type);
  if (field_id == NULL)
  {
    char log_buffer[200];
    snprintf(log_buffer, sizeof(log_buffer),
        "JNICache: Failed to find static field %s with type %s", field, type);
    Logging::fatal(log_buffer);
    perror("Failure during JNI static field id lookup. Check log file for details.");
    this->error = true;
  }
  return field_id;
}
