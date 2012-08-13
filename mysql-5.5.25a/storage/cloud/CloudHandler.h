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

typedef struct st_record_buffer {
  uchar *buffer;
  uint32 length;
} record_buffer;

class CloudHandler : public handler
{
private:
    THR_LOCK_DATA lock;      	///< MySQL lockCloudShare;
    CloudShare *share;    		///< Shared lock info
    mysql_mutex_t* cloud_mutex;
    HASH* cloud_open_tables;
    record_buffer *rec_buffer;
    record_buffer *create_record_buffer(unsigned int length);
    void destroy_record_buffer(record_buffer *r);
    CloudShare *get_share(const char *table_name, TABLE *table);

    long long curr_scan_id;

    // HBase JNI Adapter:
    JNIEnv* env;
    JavaVM* jvm;

    const char* java_to_string(jstring str);
    jstring string_to_java_string(const char*);
    jobject create_java_map();
    jobject java_map_insert(jobject java_map, jstring key, jbyteArray value);
    jbyteArray convert_value_to_java_bytes(uchar* value, uint32 length);
    void store_field_values(uchar *buf, jarray keys, jarray vals);
    void store_field_value(Field* field, uchar* buf, const char* key, char* val, jsize val_length);
    int delete_row_helper();
    int write_row_helper();
    jobject sql_to_java();

    void reverse_bytes(uchar *begin, uchar *end)
    {
      for (; begin <= end; begin++, end--)
      {
        uchar tmp = *end;
        *end = *begin;
        *begin = tmp;
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

    void make_big_endian(uchar *begin, uchar *end)
    {
      if (is_little_endian())
      {
        reverse_bytes(begin, end);
      }
    }

    longlong htonll(longlong src, bool check_endian = true) {
      const int TYP_INIT = 0;
      const int TYP_SMLE = 1;
      const int TYP_BIGE = 2;

      static int typ = TYP_INIT;
      unsigned char c;
      union {
        longlong ull;
        unsigned char c[8];
      } x;

      if (check_endian)
      {
        if (typ == TYP_INIT) {
          x.ull = 0x01;
          typ = (x.c[7] == 0x01ULL) ? TYP_BIGE : TYP_SMLE;
        }
        if (typ == TYP_BIGE)
          return src;
      }

      x.ull = src;
      c = x.c[0]; x.c[0] = x.c[7]; x.c[7] = c;
      c = x.c[1]; x.c[1] = x.c[6]; x.c[6] = c;
      c = x.c[2]; x.c[2] = x.c[5]; x.c[5] = c;
      c = x.c[3]; x.c[3] = x.c[4]; x.c[4] = c;

      return x.ull;
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
        INFO(("Exception from \n atoehutnaoeuhaotenhu"));
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
      int add_index(TABLE *table_arg, KEY *key_info, uint num_of_keys, handler_add_index **add);
      int final_add_index(handler_add_index *add, bool commit);
      int prepare_drop_index(TABLE *table_arg, uint *key_num, uint num_of_keys);
      int final_drop_index(TABLE *table_arg);
    
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
        return HA_BINLOG_STMT_CAPABLE | HA_REC_NOT_IN_SEQ;
      }

      ulong index_flags(uint inx, uint part, bool all_parts) const
      {
        return 0;
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
        return MAX_REF_PARTS; 
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

};

#endif
