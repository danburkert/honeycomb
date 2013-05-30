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


#ifndef JNICACHE_H
#define JNICACHE_H

struct JNIEnv_;
typedef JNIEnv_ JNIEnv;

struct JavaVM_;
typedef JavaVM_ JavaVM;

struct _jmethodID;
typedef _jmethodID *jmethodID;

struct _jfieldID;
typedef _jfieldID *jfieldID;

class _jclass;
typedef _jclass *jclass;


/**
 * @brief Holds jmethodID's and jclass global refs to be used in later JNI
 * invocations by Honeycomb.  Upon creation, JNICache asks the JVM for
 * the jmethodID's and jclass refs it needs, and caches them.
 */
class JNICache
{
  public:

    /**
     * @brief Holds jmethodID's and the jclass for HandlerProxy
     */
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
    /**
     * @brief Holds jmethodID's and the jclass for Throwable
     */
    struct Throwable
    {
      jclass clazz;
      jmethodID print_stack_trace;
    };
    /**
     * @brief Holds jmethodID's and the jclass for PrintWriter
     */
    struct PrintWriter
    {
      jclass clazz;
      jmethodID init;
    };
    /**
     * @brief Holds jmethodID's and the jclass for StringWriter
     */
    struct StringWriter
    {
      jclass clazz;
      jmethodID init,
                to_string;
    };
    /**
     * @brief Holds jmethodID's and the jclass for HandlerProxyFactory
     */
    struct HandlerProxyFactory
    {
      jclass clazz;
      jmethodID createHandlerProxy;
    };

  private:
    JavaVM* jvm;
    bool error;

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
    bool has_error()                            const {return error;}
    HandlerProxy handler_proxy()                const {return handler_proxy_;}
    Throwable throwable()                       const {return throwable_;}
    PrintWriter print_writer()                  const {return print_writer_;}
    StringWriter string_writer()                const {return string_writer_;}
    HandlerProxyFactory handler_proxy_factory() const {return handler_proxy_factory_;}

    jclass TableNotFoundException;
    jclass RowNotFoundException;
    jclass StorageBackendCreationException;
    jclass RuntimeIOException;
    jclass UnknownSchemaVersionException;

    JNICache(JavaVM* jvm);
    ~JNICache();
};

#endif
