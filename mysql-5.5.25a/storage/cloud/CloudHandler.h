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
    jstring string_to_java_string(JNIEnv *jni_env, const char*);
    jobject create_java_map(JNIEnv *jni_env);
    jobject java_map_insert(JNIEnv *jni_env, jobject java_map, jstring key, jbyteArray value);
    jbyteArray convert_value_to_java_bytes(JNIEnv *jni_env, uchar* value, uint32 length);
    void store_field_values(uchar *buf, jarray keys, jarray vals);
    void store_field_value(Field* field, uchar* buf, const char* key, char* val, jsize val_length);

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
        return HA_BINLOG_STMT_CAPABLE;
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
        return 0; 
      }

      uint max_supported_key_parts() const 
      {
        return 0; 
      }

      uint max_supported_key_length() const 
      {
        return 0;
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
