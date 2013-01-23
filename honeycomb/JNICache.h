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
    };
    struct IndexReadType
    {
      jclass clazz;
    };
    struct IndexRow
    {
      jclass clazz;
      jmethodID get_uuid, get_row_map;
    };
    struct Row
    {
      jclass clazz;
    };
    struct ColumnMetadata
    {
      jclass clazz;
      jmethodID init, set_max_lenth, set_precision, set_scale, set_nullable,
                set_primary_keys, set_type, set_auto_increment,
                set_auto_increment_value;
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
      jmethodID init, add_multipart_key;
    };
    struct Boolean
    {
      jclass clazz;
      jmethodID init;
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
      jmethodID get, is_empty;
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
    Boolean boolean_;
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
      char log_buffer[80]; // 80 character columns FTW
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

    //jmethodID get_method_id(JNIEnv* env, const char* method, const char* type)
    //{
      //return NULL;
    //}

  public:
    inline HBaseAdapter hbase_adapter()              const{return hbase_adapter_;};
    inline IndexReadType index_read_type()           const{return index_read_type_;};
    inline IndexRow index_row()                      const{return index_row_;};
    inline Row row()                                 const{return row_;};
    inline ColumnMetadata column_metadata()          const{return column_metadata_;};
    inline ColumnType column_type()                  const{return column_type_;};
    inline KeyValue key_value()                      const{return key_value_;};
    inline TableMultipartKeys table_multipart_keys() const{return table_multipart_keys_;};
    inline Boolean boolean()                         const{return boolean_;};
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

      hbase_adapter_.clazz        = get_class_ref(env, MYSQLENGINE "HBaseAdapter");
      index_read_type_.clazz      = get_class_ref(env, MYSQLENGINE "IndexReadType");
      index_row_.clazz            = get_class_ref(env, MYSQLENGINE "IndexRow");
      row_.clazz                  = get_class_ref(env, MYSQLENGINE "Row");
      column_metadata_.clazz      = get_class_ref(env, HBASECLIENT "ColumnMetadata");
      column_type_.clazz          = get_class_ref(env, HBASECLIENT "ColumnType");
      key_value_.clazz            = get_class_ref(env, HBASECLIENT "KeyValue");
      table_multipart_keys_.clazz = get_class_ref(env, HBASECLIENT "TableMultipartKeys");
      boolean_.clazz              = get_class_ref(env, "java/lang/Boolean");
      throwable_.clazz            = get_class_ref(env, "java/lang/Throwable");
      print_writer_.clazz         = get_class_ref(env, "java/io/PrintWriter");
      string_writer_.clazz        = get_class_ref(env, "java/io/StringWriter");
      linked_list_.clazz          = get_class_ref(env, "java/util/LinkedList");
      tree_map_.clazz             = get_class_ref(env, "java/util/TreeMap");

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
      env->DeleteGlobalRef(boolean_.clazz);
      env->DeleteGlobalRef(throwable_.clazz);
      env->DeleteGlobalRef(print_writer_.clazz);
      env->DeleteGlobalRef(string_writer_.clazz);
      env->DeleteGlobalRef(linked_list_.clazz);
      env->DeleteGlobalRef(tree_map_.clazz);

      detach_thread(jvm);
    }
};

#endif
