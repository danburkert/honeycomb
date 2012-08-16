#ifndef CLOUD_HANDLER_H
#define CLOUD_HANDLER_H

#ifdef USE_PRAGMA_INTERFACE
#pragma interface			/* gcc class implementation */
#endif

#include "my_global.h"                   /* ulonglong */
#include "thr_lock.h"                    /* THR_LOCK, THR_LOCK_DATA */
#include "handler.h"                     /* handler */
#include "my_base.h"                     /* ha_rows */
#include "CloudShare.h"
#include <jni.h>
#include "Macros.h"
#include <string.h>

typedef struct st_record_buffer {
  uchar *buffer;
  uint32 length;
} record_buffer;

enum hbase_data_type { UNKNOWN_TYPE, JAVA_STRING, JAVA_LONG, JAVA_DOUBLE, JAVA_TIME };

class CloudHandler : public handler
{
private:
    THR_LOCK_DATA lock;      	///< MySQL lockCloudShare;
    CloudShare *share;    		///< Shared lock info
    mysql_mutex_t* cloud_mutex;
    HASH* cloud_open_tables;
    record_buffer *rec_buffer;
    bool performing_scan;
    record_buffer *create_record_buffer(unsigned int length);
    void destroy_record_buffer(record_buffer *r);
    CloudShare *get_share(const char *table_name, TABLE *table);
    uint32 max_row_length();
    void unpack_index(uchar* buf, jbyteArray uniReg);
    jobject java_find_flag(enum ha_rkey_function find_flag);
    int index_field_type;

    long long curr_scan_id;

    // HBase JNI Adapter:
    JNIEnv* env;
    JavaVM* jvm;

    const char* java_to_string(jstring str);
    jstring string_to_java_string(const char*);
    jobject create_java_map();
    jobject java_map_insert(jobject java_map, jobject key, jobject value);
    jbyteArray java_map_get(jobject java_map, jstring key);
    jboolean java_map_is_empty(jobject java_map);
    jbyteArray convert_value_to_java_bytes(uchar* value, uint32 length);
    jobject get_field_metadata(Field *field, TABLE *table_arg);
    hbase_data_type extract_field_type(Field *field);
    void java_to_sql(uchar *buf, jobject row_map);
    int delete_row_helper();
    int write_row_helper(uchar* buf);
    int bulk_write_row_helper();
    jobject sql_to_java();
    int delete_all_rows();
    bool start_bulk_delete();
    int end_bulk_delete();
    int delete_table(const char *name);
    void drop_table(const char *name);
    int truncate();
    jobject create_java_list();
    void java_list_add(jobject list, jobject obj);
    jobject create_metadata_enum_object(const char *name);

    void reverse_bytes(uchar *begin, uint length)
    {
      for(int x = 0, y = length - 1; x < y; x++, y--)
      {
        uchar tmp = begin[x];
        begin[x] = begin[y];
        begin[y] = tmp;
      }
    }

    bool is_little_endian()
    {
      union
      {
        uint32_t i;
        char c[4];
      } bint = {0x01020304};

      return bint.c[0] == 4;
    }

    void make_big_endian(uchar *begin, uint length)
    {
      if (is_little_endian())
      {
        reverse_bytes(begin, length);
      }
    }

    bool is_integral_field(int field_type)
    {
      return (field_type == MYSQL_TYPE_LONG
          || field_type == MYSQL_TYPE_SHORT
          || field_type == MYSQL_TYPE_TINY
          || field_type == MYSQL_TYPE_LONGLONG
          || field_type == MYSQL_TYPE_INT24
          || field_type == MYSQL_TYPE_ENUM
          || field_type == MYSQL_TYPE_YEAR);
    }

    // For those annoying times when you need the table name but actually have its file path
    const char *extract_table_name_from_path(const char *path)
    {
    	const char *ptr = path + strlen(path);

    	while(*(ptr-1) != '/')
    	{
    		ptr--;
    	}

    	return ptr;
    }

    void print_java_exception(JNIEnv* jni_env)
    {
      if(jni_env->ExceptionCheck() == JNI_TRUE)
      {
        jthrowable throwable = jni_env->ExceptionOccurred();
        jclass objClazz = jni_env->GetObjectClass(throwable);
        jmethodID methodId = jni_env->GetMethodID(objClazz, "toString", "()Ljava/lang/String;");
        jstring result = (jstring)jni_env->CallObjectMethod(throwable, methodId);
        const char* string = jni_env->GetStringUTFChars(result, NULL);
        INFO(("Exception from java: %s", string));
        jni_env->ReleaseStringUTFChars(result, string);
      }
    }

      /* Index methods */
      int index_init(uint idx, bool sorted);
      int index_end();
      //int disable_indexes(uint mode);
      //int enable_indexes(uint mode);
      int index_read(uchar *buf, const uchar *key, uint key_len, enum ha_rkey_function find_flag);
      //int index_read_last(uchar *buf, const uchar *key, uint key_len);
      int index_next(uchar *buf);
      int index_prev(uchar *buf);
      int index_first(uchar *buf);
      int index_last(uchar *buf);
      //int index_next_same(uchar *buf, const uchar *key, uint keylen);
    
    public:
      CloudHandler(handlerton *hton, TABLE_SHARE *table_arg, mysql_mutex_t* mutex, HASH* open_tables, JavaVM* jvm)
        : handler(hton, table_arg), jvm(jvm)
      {
    	  cloud_mutex = mutex;
        cloud_open_tables = open_tables;
      }

      const char *table_type() const 
      {
        return "cloud";
      }

      const char *index_type(uint inx) 
      {
        return "HASH";
      }

      ulonglong table_flags() const
      {
        return HA_FAST_KEY_READ |
          HA_BINLOG_STMT_CAPABLE |
          HA_REC_NOT_IN_SEQ |
          HA_NO_TRANSACTIONS |
          HA_NULL_IN_KEY; // Nulls in indexed columns are allowed
      }

      ulong index_flags(uint inx, uint part, bool all_parts) const
      {
        return HA_READ_NEXT | HA_READ_ORDER | HA_READ_RANGE | HA_READ_PREV;
      }

      uint max_supported_record_length() const 
      {
        return HA_MAX_REC_LENGTH; 
      }

      uint max_supported_keys() const 
      {
        return 1; 
      }

      uint max_supported_key_parts() const 
      {
        return 1; 
      }

      uint max_supported_key_length() const 
      {
        return 255;
      }

      virtual double scan_time() 
      { 
        return (double) (stats.records+stats.deleted) / 20.0+10; 
      }

      virtual double read_time(uint, uint, ha_rows rows)
      { 
        return (double) rows /  20.0+1; 
      }

      const char **bas_ext() const;
      int open(const char *name, int mode, uint test_if_locked);    // required
      int close(void);                                              // required
      int rnd_init(bool scan);                                      //required
      int rnd_next(uchar *buf);                                     ///< required
      int rnd_pos(uchar *buf, uchar *pos);                          ///< required
      void position(const uchar *record);                           ///< required
      int info(uint);                                               ///< required
      int external_lock(THD *thd, int lock_type);                   ///< required
      int create(const char *name, TABLE *form, HA_CREATE_INFO *create_info); ///< required
      THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type);     ///< required
      int extra(enum ha_extra_function operation);
      int update_row(const uchar *old_data, uchar *new_data);
      int write_row(uchar *buf);
      int delete_row(const uchar *buf);
      int free_share(CloudShare *share);
      int rnd_end();
      void start_bulk_insert(ha_rows rows);
      int end_bulk_insert();
};

#endif
