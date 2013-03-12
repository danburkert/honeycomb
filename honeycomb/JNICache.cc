#include "JNICache.h"
#include "JNISetup.h"
#include "JavaFrame.h"
#include "Logging.h"
#include "Macros.h"
JNICache::JNICache(JavaVM* jvm) : jvm(jvm)
{
  JNIEnv* env;
  jint attach_result = attach_thread(jvm, env);
  CHECK_JNI_ABORT(attach_result, "JNICache: Failure while attaching thread to JVM.");

  // (dburkert:) I do not recommend editing this section without javap -s,
  // editor macros, and tabular.vim
  hbase_adapter_.clazz                        = get_class_ref(env, MYSQLENGINE "HBaseAdapter");
  hbase_adapter_.initialize                   = get_static_method_id(env, hbase_adapter_.clazz, "initialize", "()V");
  hbase_adapter_.create_table                 = get_static_method_id(env, hbase_adapter_.clazz, "createTable", "(Ljava/lang/String;Ljava/util/Map;Lcom/nearinfinity/honeycomb/hbaseclient/TableMultipartKeys;)Z");
  hbase_adapter_.get_autoincrement_value      = get_static_method_id(env, hbase_adapter_.clazz, "getAutoincrementValue", "(Ljava/lang/String;Ljava/lang/String;)J");
  hbase_adapter_.alter_autoincrement_value    = get_static_method_id(env, hbase_adapter_.clazz, "alterAutoincrementValue", "(Ljava/lang/String;Ljava/lang/String;JZ)Z");
  hbase_adapter_.start_write                  = get_static_method_id(env, hbase_adapter_.clazz, "startWrite", "()J");
  hbase_adapter_.end_write                    = get_static_method_id(env, hbase_adapter_.clazz, "endWrite", "(J)V");
  hbase_adapter_.start_scan                   = get_static_method_id(env, hbase_adapter_.clazz, "startScan", "(Ljava/lang/String;Z)J");
  hbase_adapter_.next_row                     = get_static_method_id(env, hbase_adapter_.clazz, "nextRow", "(J)[B");
  hbase_adapter_.end_scan                     = get_static_method_id(env, hbase_adapter_.clazz, "endScan", "(J)V");
  hbase_adapter_.write_row                    = get_static_method_id(env, hbase_adapter_.clazz, "writeRow", "(JLjava/lang/String;Ljava/util/Map;)Z");
  hbase_adapter_.update_row                   = get_static_method_id(env, hbase_adapter_.clazz, "updateRow", "(J[BLjava/util/List;Ljava/lang/String;Ljava/util/Map;)V");
  hbase_adapter_.flush_writes                 = get_static_method_id(env, hbase_adapter_.clazz, "flushWrites", "(J)V");
  hbase_adapter_.delete_row                   = get_static_method_id(env, hbase_adapter_.clazz, "deleteRow", "(Ljava/lang/String;[B)Z");
  hbase_adapter_.delete_all_rows              = get_static_method_id(env, hbase_adapter_.clazz, "deleteAllRows", "(Ljava/lang/String;)I");
  hbase_adapter_.drop_table                   = get_static_method_id(env, hbase_adapter_.clazz, "dropTable", "(Ljava/lang/String;)Z");
  hbase_adapter_.get_row                      = get_static_method_id(env, hbase_adapter_.clazz, "getRow", "(J[B)[B");
  hbase_adapter_.start_index_scan             = get_static_method_id(env, hbase_adapter_.clazz, "startIndexScan", "(Ljava/lang/String;Ljava/lang/String;)J");
  hbase_adapter_.find_duplicate_key           = get_static_method_id(env, hbase_adapter_.clazz, "findDuplicateKey", "(Ljava/lang/String;Ljava/util/Map;)Ljava/lang/String;");
  hbase_adapter_.find_duplicate_key_list      = get_static_method_id(env, hbase_adapter_.clazz, "findDuplicateKey", "(Ljava/lang/String;Ljava/util/Map;Ljava/util/List;)Ljava/lang/String;");
  hbase_adapter_.find_duplicate_value         = get_static_method_id(env, hbase_adapter_.clazz, "findDuplicateValue", "(Ljava/lang/String;Ljava/lang/String;)[B");
  hbase_adapter_.get_next_autoincrement_value = get_static_method_id(env, hbase_adapter_.clazz, "getNextAutoincrementValue", "(Ljava/lang/String;Ljava/lang/String;)J");
  hbase_adapter_.index_read                   = get_static_method_id(env, hbase_adapter_.clazz, "indexRead", "(JLjava/util/List;Lcom/nearinfinity/honeycomb/mysqlengine/IndexReadType;)[B");
  hbase_adapter_.next_index_row               = get_static_method_id(env, hbase_adapter_.clazz, "nextIndexRow", "(J)[B");
  hbase_adapter_.increment_row_count          = get_static_method_id(env, hbase_adapter_.clazz, "incrementRowCount", "(Ljava/lang/String;J)V");
  hbase_adapter_.set_row_count                = get_static_method_id(env, hbase_adapter_.clazz, "setRowCount", "(Ljava/lang/String;J)V");
  hbase_adapter_.get_row_count                = get_static_method_id(env, hbase_adapter_.clazz, "getRowCount", "(Ljava/lang/String;)J");
  hbase_adapter_.rename_table                 = get_static_method_id(env, hbase_adapter_.clazz, "renameTable", "(Ljava/lang/String;Ljava/lang/String;)V");
  hbase_adapter_.is_nullable                  = get_static_method_id(env, hbase_adapter_.clazz, "isNullable", "(Ljava/lang/String;Ljava/lang/String;)Z");
  hbase_adapter_.add_index                    = get_static_method_id(env, hbase_adapter_.clazz, "addIndex", "(Ljava/lang/String;Lcom/nearinfinity/honeycomb/hbaseclient/TableMultipartKeys;)V");
  hbase_adapter_.drop_index                   = get_static_method_id(env, hbase_adapter_.clazz, "dropIndex", "(Ljava/lang/String;Ljava/lang/String;)V");

  index_read_type_.clazz            = get_class_ref(env, MYSQLENGINE "IndexReadType");
  index_read_type_.READ_KEY_EXACT   = get_static_field_id(env, index_read_type_.clazz, "HA_READ_KEY_EXACT", "L" MYSQLENGINE "IndexReadType;");
  index_read_type_.READ_AFTER_KEY   = get_static_field_id(env, index_read_type_.clazz, "HA_READ_AFTER_KEY", "L" MYSQLENGINE "IndexReadType;");
  index_read_type_.READ_KEY_OR_NEXT = get_static_field_id(env, index_read_type_.clazz, "HA_READ_KEY_OR_NEXT", "L" MYSQLENGINE "IndexReadType;");
  index_read_type_.READ_KEY_OR_PREV = get_static_field_id(env, index_read_type_.clazz, "HA_READ_KEY_OR_PREV", "L" MYSQLENGINE "IndexReadType;");
  index_read_type_.READ_BEFORE_KEY  = get_static_field_id(env, index_read_type_.clazz, "HA_READ_BEFORE_KEY", "L" MYSQLENGINE "IndexReadType;");
  index_read_type_.INDEX_FIRST      = get_static_field_id(env, index_read_type_.clazz, "INDEX_FIRST", "L" MYSQLENGINE "IndexReadType;");
  index_read_type_.INDEX_LAST       = get_static_field_id(env, index_read_type_.clazz, "INDEX_LAST", "L" MYSQLENGINE "IndexReadType;");
  index_read_type_.INDEX_NULL       = get_static_field_id(env, index_read_type_.clazz, "INDEX_NULL", "L" MYSQLENGINE "IndexReadType;");

  row_.clazz       = get_class_ref(env, "com/nearinfinity/honeycomb/mysql/Row");
  row_.get_row_map = get_method_id(env, row_.clazz, "getRowMap", "()Ljava/util/Map;");
  row_.get_uuid    = get_method_id(env, row_.clazz, "getUUIDBuffer", "()[B");

  column_metadata_.clazz                   = get_class_ref(env, HBASECLIENT "ColumnMetadata");
  column_metadata_.init                    = get_method_id(env, column_metadata_.clazz, "<init>", "()V");
  column_metadata_.set_max_length          = get_method_id(env, column_metadata_.clazz, "setMaxLength", "(I)V");
  column_metadata_.set_precision           = get_method_id(env, column_metadata_.clazz, "setPrecision", "(I)V");
  column_metadata_.set_scale               = get_method_id(env, column_metadata_.clazz, "setScale", "(I)V");
  column_metadata_.set_nullable            = get_method_id(env, column_metadata_.clazz, "setNullable", "(Z)V");
  column_metadata_.set_primary_key         = get_method_id(env, column_metadata_.clazz, "setPrimaryKey", "(Z)V");
  column_metadata_.set_type                = get_method_id(env, column_metadata_.clazz, "setType", "(L" HBASECLIENT "ColumnType;)V");
  column_metadata_.set_autoincrement       = get_method_id(env, column_metadata_.clazz, "setAutoincrement", "(Z)V");
  column_metadata_.set_autoincrement_value = get_method_id(env, column_metadata_.clazz, "setAutoincrementValue", "(J)V");

  column_type_.clazz    = get_class_ref(env, HBASECLIENT "ColumnType");
  column_type_.NONE     = get_static_field_id(env, column_type_.clazz, "NONE", "L" HBASECLIENT "ColumnType;");
  column_type_.STRING   = get_static_field_id(env, column_type_.clazz, "STRING", "L" HBASECLIENT "ColumnType;");
  column_type_.BINARY   = get_static_field_id(env, column_type_.clazz, "BINARY", "L" HBASECLIENT "ColumnType;");
  column_type_.ULONG    = get_static_field_id(env, column_type_.clazz, "ULONG", "L" HBASECLIENT "ColumnType;");
  column_type_.LONG     = get_static_field_id(env, column_type_.clazz, "LONG", "L" HBASECLIENT "ColumnType;");
  column_type_.DOUBLE   = get_static_field_id(env, column_type_.clazz, "DOUBLE", "L" HBASECLIENT "ColumnType;");
  column_type_.TIME     = get_static_field_id(env, column_type_.clazz, "TIME", "L" HBASECLIENT "ColumnType;");
  column_type_.DATE     = get_static_field_id(env, column_type_.clazz, "DATE", "L" HBASECLIENT "ColumnType;");
  column_type_.DATETIME = get_static_field_id(env, column_type_.clazz, "DATETIME", "L" HBASECLIENT "ColumnType;");
  column_type_.DECIMAL  = get_static_field_id(env, column_type_.clazz, "DECIMAL", "L" HBASECLIENT "ColumnType;");

  key_value_.clazz = get_class_ref(env, HBASECLIENT "KeyValue");
  key_value_.init  = get_method_id(env, key_value_.clazz, "<init>", "(Ljava/lang/String;[BZZ)V");

  table_multipart_keys_.clazz             = get_class_ref(env, HBASECLIENT "TableMultipartKeys");
  table_multipart_keys_.init              = get_method_id(env, table_multipart_keys_.clazz, "<init>", "()V");
  table_multipart_keys_.add_multipart_key = get_method_id(env, table_multipart_keys_.clazz, "addMultipartKey", "(Ljava/lang/String;Z)V");

  throwable_.clazz             = get_class_ref(env, "java/lang/Throwable");
  throwable_.print_stack_trace = get_method_id(env, throwable_.clazz, "printStackTrace", "(Ljava/io/PrintWriter;)V");

  print_writer_.clazz = get_class_ref(env, "java/io/PrintWriter");
  print_writer_.init  = get_method_id(env, print_writer_.clazz, "<init>", "(Ljava/io/Writer;)V");

  string_writer_.clazz     = get_class_ref(env, "java/io/StringWriter");
  string_writer_.init      = get_method_id(env, string_writer_.clazz, "<init>", "()V");
  string_writer_.to_string = get_method_id(env, string_writer_.clazz, "toString", "()Ljava/lang/String;");

  linked_list_.clazz = get_class_ref(env, "java/util/LinkedList");
  linked_list_.init  = get_method_id(env, linked_list_.clazz, "<init>", "()V");
  linked_list_.add   = get_method_id(env, linked_list_.clazz, "add", "(Ljava/lang/Object;)Z");
  linked_list_.size  = get_method_id(env, linked_list_.clazz, "size", "()I");

  tree_map_.clazz    = get_class_ref(env, "java/util/TreeMap");
  tree_map_.init     = get_method_id(env, tree_map_.clazz, "<init>", "()V");
  tree_map_.get      = get_method_id(env, tree_map_.clazz, "get", "(Ljava/lang/Object;)Ljava/lang/Object;");
  tree_map_.put      = get_method_id(env, tree_map_.clazz, "put", "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");
  tree_map_.is_empty = get_method_id(env, tree_map_.clazz, "isEmpty", "()Z");

  handler_proxy_factory_.clazz              = get_class_ref(env, "com/nearinfinity/honeycomb/mysql/HandlerProxyFactory");
  handler_proxy_factory_.createHandlerProxy = get_method_id(env, handler_proxy_factory_.clazz, "createHandlerProxy", "()Lcom/nearinfinity/honeycomb/mysql/HandlerProxy;");

  detach_thread(jvm);
}

JNICache::~JNICache()
{
  // Setup env
  JNIEnv* env;
  jint attach_result = attach_thread(jvm, env);
  CHECK_JNI_ABORT(attach_result, "JNICache Destructor: Failure while attaching thread to JVM.");

  env->DeleteGlobalRef(hbase_adapter_.clazz);
  env->DeleteGlobalRef(index_read_type_.clazz);
  env->DeleteGlobalRef(row_.clazz);
  env->DeleteGlobalRef(column_metadata_.clazz);
  env->DeleteGlobalRef(column_type_.clazz);
  env->DeleteGlobalRef(key_value_.clazz);
  env->DeleteGlobalRef(table_multipart_keys_.clazz);
  env->DeleteGlobalRef(throwable_.clazz);
  env->DeleteGlobalRef(print_writer_.clazz);
  env->DeleteGlobalRef(string_writer_.clazz);
  env->DeleteGlobalRef(linked_list_.clazz);
  env->DeleteGlobalRef(tree_map_.clazz);

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
