#ifndef JNICACHE_H
#define JNICACHE_H

#include "JNISetup.h"
#include "JavaFrame.h"
#include "Macros.h"

/* JNICache holds jmethodID's and jclass global refs to be used in later JNI
 * invocations by Honeycomb.  Upon creation, JNICache asks the JVM for
 * the jmethodID's and jclass refs it needs, and stores them in its fields.
 */
class JNICache
{
  private:
    JavaVM* jvm;
    jclass hbase_adapter_;
    jclass column_type_;
    jclass column_metadata_;
    jclass boolean_;
    jclass tree_map_;
    jclass linked_list_;
    jclass throwable_;
    jclass string_writer_;
    jclass print_writer_;
    jclass table_multipart_keys_;
    jclass key_value_;

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

  public:
    inline jclass hbase_adapter()        const {return hbase_adapter_;};
    inline jclass column_type()          const {return column_type_;};
    inline jclass column_metadata()      const {return column_metadata_;};
    inline jclass table_multipart_keys() const {return table_multipart_keys_;};
    inline jclass key_value()            const {return key_value_;};
    inline jclass boolean()              const {return boolean_;};
    inline jclass tree_map()             const {return tree_map_;};
    inline jclass linked_list()          const {return linked_list_;};
    inline jclass throwable()            const {return throwable_;};
    inline jclass string_writer()        const {return string_writer_;};
    inline jclass print_writer()         const {return print_writer_;};

    JNICache(JavaVM* jvm) : jvm(jvm)
    {
      JNIEnv* env;
      jint attach_result = attach_thread(jvm, env);
      CHECK_JNI_ABORT(attach_result, "JNICache: Failure while attaching thread to JVM.");

      hbase_adapter_        = get_class_ref(env, MYSQLENGINE "HBaseAdapter");
      column_type_          = get_class_ref(env, HBASECLIENT "ColumnType");
      column_metadata_      = get_class_ref(env, HBASECLIENT "ColumnMetadata");
      table_multipart_keys_ = get_class_ref(env, HBASECLIENT "TableMultipartKeys");
      key_value_            = get_class_ref(env, HBASECLIENT "KeyValue");
      boolean_              = get_class_ref(env, "java/lang/Boolean");
      tree_map_             = get_class_ref(env, "java/util/TreeMap");
      linked_list_          = get_class_ref(env, "java/util/LinkedList");
      throwable_            = get_class_ref(env, "java/lang/Throwable");
      string_writer_        = get_class_ref(env, "java/io/StringWriter");
      print_writer_         = get_class_ref(env, "java/io/PrintWriter");

      detach_thread(jvm);
    }

    ~JNICache()
    {
      // Setup env
      JNIEnv* env;
      jint attach_result = attach_thread(jvm, env);
      CHECK_JNI_ABORT(attach_result, "JNICache Destructor: Failure while attaching thread to JVM.");

      env->DeleteGlobalRef(hbase_adapter_);
      env->DeleteGlobalRef(hbase_adapter_);
      env->DeleteGlobalRef(column_type_);
      env->DeleteGlobalRef(column_metadata_);
      env->DeleteGlobalRef(boolean_);
      env->DeleteGlobalRef(tree_map_);
      env->DeleteGlobalRef(linked_list_);
      env->DeleteGlobalRef(throwable_);
      env->DeleteGlobalRef(string_writer_);
      env->DeleteGlobalRef(print_writer_);
      env->DeleteGlobalRef(table_multipart_keys_);
      env->DeleteGlobalRef(key_value_);

      detach_thread(jvm);
    }
};

#endif
