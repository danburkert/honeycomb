#ifndef JNICACHE_H
#define JNICACHE_H

#include "JNISetup.h"
#include "JavaFrame.h"
#include "Macros.h"

/* JNICache holds jmethodID's and jclass global refs to be used in later JNI
 * invocations by Honeycomb.  Upon creation, JNICache asks the JVM for
 * the jmethodID's and jclass refs it needs, and caches them.
 */
class JNICache
{
  public:
    struct HBaseAdapter
    {
      jclass clazz;
      jmethodID start_index_scan,
        add_index, create_table, index_read;
    };
    struct IndexReadType
    {
      jclass clazz;
      jfieldID read_key_exact, read_after_key, read_key_or_next,
               read_key_or_prev, read_before_key, index_first,
               index_last, index_null;
    };
    struct IndexRow
    {
      jclass clazz;
      jmethodID get_row_map, get_uuid;
    };
    struct Row
    {
      jclass clazz;
      jmethodID get_row_map, get_uuid;
    };
    struct ColumnMetadata
    {
      jclass clazz;
      jmethodID init, set_max_length, set_precision, set_scale, set_nullable,
                set_primary_key, set_type, set_autoincrement,
                set_autoincrement_value;
    };
    struct ColumnType
    {
      jclass clazz;
    };
    struct KeyValue
    {
      jclass clazz;
      jmethodID init;
    };
    struct TableMultipartKeys
    {
      jclass clazz;
      jmethodID init, add_index, create_table, add_multipart_key;
    };
    struct Throwable
    {
      jclass clazz;
      jmethodID print_stack_trace;
    };
    struct PrintWriter
    {
      jclass clazz;
      jmethodID init;
    };
    struct StringWriter
    {
      jclass clazz;
      jmethodID init, to_string;
    };
    struct LinkedList
    {
      jclass clazz;
      jmethodID init, add, size;
    };
    struct TreeMap
    {
      jclass clazz;
      jmethodID init, get, put, is_empty;
    };

  private:
    JavaVM* jvm;

    HBaseAdapter hbase_adapter_;
    IndexReadType index_read_type_;
    IndexRow index_row_;
    Row row_;
    ColumnMetadata column_metadata_;
    ColumnType column_type_;
    KeyValue key_value_;
    TableMultipartKeys table_multipart_keys_;
    Throwable throwable_;
    PrintWriter print_writer_;
    StringWriter string_writer_;
    LinkedList linked_list_;
    TreeMap tree_map_;

    /**
     * Find class id of clazz in env, and return a global reference to it.
     * Abort if the class is not found, or if there is not enough memory
     * to create references to it.
     */
    jclass get_class_ref(JNIEnv* env, const char* clazz)
    {
      char log_buffer[200];
      JavaFrame frame(env, 1);
      jclass local_clazz_ref = env->FindClass(clazz);
      if (local_clazz_ref == NULL)
      {
        snprintf(log_buffer, sizeof(log_buffer),
            "JNICache: Failed to find class %s", clazz);
        Logging::fatal(log_buffer);
        perror("Failure during JNI class lookup. Check honeycomb.log for details.");
        abort();
      }
      jclass clazz_ref = (jclass) env->NewGlobalRef(local_clazz_ref);
      if (clazz_ref == NULL)
      {
        snprintf(log_buffer, sizeof(log_buffer),
            "JNICache: Not enough JVM memory to create global reference to class %s", clazz);
        Logging::fatal(log_buffer);
        perror("Failure during JNI reference creation. Check honeycomb.log for details.");
        abort();
      }
      return clazz_ref;
    }

    jmethodID get_method_id(JNIEnv* env, jclass clazz, const char* method, const char* signature)
    {
      char log_buffer[200];
      jmethodID method_id = env->GetMethodID(clazz, method, signature);
      if (method_id == NULL)
      {
        snprintf(log_buffer, sizeof(log_buffer),
            "JNICache: Failed to find method %s with signature %s", method, signature);
        Logging::fatal(log_buffer);
        perror("Failure during JNI method id lookup. Check honeycomb.log for details.");
        abort();
      }
      return method_id;
    }

    jfieldID get_static_field_id(JNIEnv* env, jclass clazz, const char* field, const char* signature)
    {
      char log_buffer[200];
      jfieldID field_id = env->GetStaticFieldID(clazz, field, signature);
      if (field_id == NULL)
      {
        snprintf(log_buffer, sizeof(log_buffer),
            "JNICache: Failed to find static field %s with signature %s", field, signature);
        Logging::fatal(log_buffer);
        perror("Failure during JNI static field id lookup. Check honeycomb.log for details.");
        abort();
      }
      return field_id;
    }

  public:
    inline HBaseAdapter hbase_adapter()              const{return hbase_adapter_;};
    inline IndexReadType index_read_type()           const{return index_read_type_;};
    inline IndexRow index_row()                      const{return index_row_;};
    inline Row row()                                 const{return row_;};
    inline ColumnMetadata column_metadata()          const{return column_metadata_;};
    inline ColumnType column_type()                  const{return column_type_;};
    inline KeyValue key_value()                      const{return key_value_;};
    inline TableMultipartKeys table_multipart_keys() const{return table_multipart_keys_;};
    inline Throwable throwable()                     const{return throwable_;};
    inline PrintWriter print_writer()                const{return print_writer_;};
    inline StringWriter string_writer()              const{return string_writer_;};
    inline LinkedList linked_list()                  const{return linked_list_;};
    inline TreeMap tree_map()                        const{return tree_map_;};

    JNICache(JavaVM* jvm) : jvm(jvm)
    {
      JNIEnv* env;
      jint attach_result = attach_thread(jvm, env);
      CHECK_JNI_ABORT(attach_result, "JNICache: Failure while attaching thread to JVM.");

      hbase_adapter_.clazz            = get_class_ref(env, MYSQLENGINE "HBaseAdapter");
      //hbase_adapter_.start_index_scan = get_method_id(env, hbase_adapter_.clazz, "startIndexScan", "(Ljava/lang/String;Ljava/lang/String;)J");
      //hbase_adapter_.add_index        = get_method_id(env, hbase_adapter_.clazz, "addIndex", "(Ljava/lang/String;L" HBASECLIENT "TableMultipartKeys;)V");
      //hbase_adapter_.create_table     = get_method_id(env, hbase_adapter_.clazz, "createTable", "(Ljava/lang/String;Ljava/util/Map;L" HBASECLIENT "TableMultipartKeys;)Z");
      //hbase_adapter_.index_read       = get_method_id(env, hbase_adapter_.clazz, "indexRead", "(JLjava/util/List;L" MYSQLENGINE "IndexReadType;)L" MYSQLENGINE "IndexRow;");

      index_read_type_.clazz            = get_class_ref(env, MYSQLENGINE "IndexReadType");
      index_read_type_.read_key_exact   = get_static_field_id(env, index_read_type_.clazz, "HA_READ_KEY_EXACT", "L" MYSQLENGINE "IndexReadType;");
      index_read_type_.read_after_key   = get_static_field_id(env, index_read_type_.clazz, "HA_READ_AFTER_KEY", "L" MYSQLENGINE "IndexReadType;");
      index_read_type_.read_key_or_next = get_static_field_id(env, index_read_type_.clazz, "HA_READ_KEY_OR_NEXT", "L" MYSQLENGINE "IndexReadType;");
      index_read_type_.read_key_or_prev = get_static_field_id(env, index_read_type_.clazz, "HA_READ_KEY_OR_PREV", "L" MYSQLENGINE "IndexReadType;");
      index_read_type_.read_before_key  = get_static_field_id(env, index_read_type_.clazz, "HA_READ_BEFORE_KEY", "L" MYSQLENGINE "IndexReadType;");
      index_read_type_.index_first      = get_static_field_id(env, index_read_type_.clazz, "INDEX_FIRST", "L" MYSQLENGINE "IndexReadType;");
      index_read_type_.index_last       = get_static_field_id(env, index_read_type_.clazz, "INDEX_LAST", "L" MYSQLENGINE "IndexReadType;");
      index_read_type_.index_null       = get_static_field_id(env, index_read_type_.clazz, "INDEX_NULL", "L" MYSQLENGINE "IndexReadType;");

      index_row_.clazz       = get_class_ref(env, MYSQLENGINE "IndexRow");
      index_row_.get_row_map = get_method_id(env, index_row_.clazz, "getRowMap", "()Ljava/util/Map;");
      index_row_.get_uuid    = get_method_id(env, index_row_.clazz, "getUUID", "()[B");

      row_.clazz       = get_class_ref(env, MYSQLENGINE "Row");
      row_.get_row_map = get_method_id(env, row_.clazz, "getRowMap", "()Ljava/util/Map;");
      row_.get_uuid    = get_method_id(env, row_.clazz, "getUUID", "()[B");

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

      column_type_.clazz = get_class_ref(env, HBASECLIENT "ColumnType");

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

      detach_thread(jvm);
    }

    ~JNICache()
    {
      // Setup env
      JNIEnv* env;
      jint attach_result = attach_thread(jvm, env);
      CHECK_JNI_ABORT(attach_result, "JNICache Destructor: Failure while attaching thread to JVM.");

      env->DeleteGlobalRef(hbase_adapter_.clazz);
      env->DeleteGlobalRef(index_read_type_.clazz);
      env->DeleteGlobalRef(index_row_.clazz);
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
};

#endif
