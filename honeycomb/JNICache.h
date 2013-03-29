#ifndef JNICACHE_H
#define JNICACHE_H

#include <jni.h>

/* JNICache holds jmethodID's and jclass global refs to be used in later JNI
 * invocations by Honeycomb.  Upon creation, JNICache asks the JVM for
 * the jmethodID's and jclass refs it needs, and caches them.
 */
class JNICache
{
  public:
    struct HandlerProxy
    {
      jclass clazz;
      jmethodID create_table,
                drop_table,
                open_table,
                close_table,
                rename_table,
                get_row_count,
                start_index_scan,
                get_next_row,
                flush,
                end_scan,
                add_index,
                drop_index,
                index_contains_duplicate,
                insert_row,
                update_row,
                delete_row,
                delete_all_rows,
                truncate_table,
                start_table_scan,
                get_row,
                increment_row_count,
                get_auto_increment,
                set_auto_increment,
                increment_auto_increment;
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
      jmethodID init,
                to_string;
    };
    struct HandlerProxyFactory
    {
      jclass clazz;
      jmethodID createHandlerProxy;
    };

  private:
    JavaVM* jvm;

    HandlerProxy handler_proxy_;
    Throwable throwable_;
    PrintWriter print_writer_;
    StringWriter string_writer_;
    HandlerProxyFactory handler_proxy_factory_;

    jclass get_class_ref(JNIEnv* env, const char* clazz);
    jmethodID get_method_id(JNIEnv* env, jclass clazz, const char* method, const char* signature);
    jmethodID get_static_method_id(JNIEnv* env, jclass clazz, const char* method, const char* signature);
    jfieldID get_static_field_id(JNIEnv* env, jclass clazz, const char* field, const char* type);

  public:
    HandlerProxy handler_proxy()                const {return handler_proxy_;}
    Throwable throwable()                       const {return throwable_;}
    PrintWriter print_writer()                  const {return print_writer_;}
    StringWriter string_writer()                const {return string_writer_;}
    HandlerProxyFactory handler_proxy_factory() const {return handler_proxy_factory_;}

    jclass TableNotFoundException;
    jclass TableExistsException;
    jclass RowNotFoundException;
    jclass StoreNotFoundException;
    jclass RuntimeIOException;

    JNICache(JavaVM* jvm);
    ~JNICache();
};

#endif
