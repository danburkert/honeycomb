#ifndef CLOUD_HANDLER_H
#define CLOUD_HANDLER_H

#ifdef USE_PRAGMA_INTERFACE
#pragma interface			/* gcc class implementation */
#endif

#include "my_global.h"                   /* ulonglong */
#include "thr_lock.h"                    /* THR_LOCK, THR_LOCK_DATA */
#include "handler.h"                     /* handler */
#include "my_base.h"                     /* ha_rows */
#include <jni.h>
#include <string.h>

#include "CloudShare.h"
#include "Macros.h"
#include "FieldMetadata.h"
#include "Util.h"
#include "Logging.h"
#include "Java.h"

typedef struct st_record_buffer {
  uchar *buffer;
  uint32 length;
} record_buffer;

static __thread int thread_ref_count=0;

class CloudHandler : public handler
{
  private:
    THR_LOCK_DATA lock;      	///< MySQL lockCloudShare;
    CloudShare *share;    		///< Sharedclass lock info
    mysql_mutex_t* cloud_mutex;
    HASH* cloud_open_tables;
    record_buffer *rec_buffer;
    bool performing_scan;
    record_buffer *create_record_buffer(unsigned int length);
    void destroy_record_buffer(record_buffer *r);
    CloudShare *get_share(const char *table_name, TABLE *table);
    uint32 max_row_length();
    Field *index_field;

    long long curr_scan_id;
    ulonglong rows_written;
    long long rows_deleted;

    // HBase JNI Adapter:
    JNIEnv* env;
    JavaVM* jvm;
    jclass hbase_adapter;

    jstring table_name();
    const char* java_to_string(jstring str);
    jstring string_to_java_string(const char *string);
    jbyteArray convert_value_to_java_bytes(uchar* value, uint32 length);
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
    bool is_key_null(const uchar *key);
    void attach_thread();
    void detach_thread();
    void store_uuid_ref(jobject index_row, jmethodID get_uuid_method);
    void bytes_to_long(const uchar* buff, unsigned int buff_length, bool is_signed, uchar* long_buff);
    int read_index_row(jobject index_row, uchar* buf);
    jobject get_index_row(const char* indexType);
    jobject get_next_index_row();
    void flush_writes();
    void end_scan();
    void reset_index_scan_counter();
    void reset_scan_counter();

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

    bool is_date_or_time_field(int field_type)
    {
      return (field_type == MYSQL_TYPE_DATE
          || field_type == MYSQL_TYPE_DATETIME
          || field_type == MYSQL_TYPE_TIME
          || field_type == MYSQL_TYPE_TIMESTAMP
          || field_type == MYSQL_TYPE_NEWDATE);
    }

    jclass adapter()
    {
      return find_jni_class("HBaseAdapter", this->env);
    }

    void initialize_adapter()
    {
      attach_thread();
      jclass adapter_class = this->adapter();
      jmethodID initialize_method = this->env->GetStaticMethodID(adapter_class, "initialize", "()V");
      this->env->CallStaticVoidMethod(adapter_class, initialize_method);
      detach_thread();
    }

    /* Index methods */
    int index_init(uint idx, bool sorted);
    int index_end();
    int index_read(uchar *buf, const uchar *key, uint key_len, enum ha_rkey_function find_flag);
    int index_next(uchar *buf);
    int index_prev(uchar *buf);
    int index_first(uchar *buf);
    int index_last(uchar *buf);

  public:
    CloudHandler(handlerton *hton, TABLE_SHARE *table_arg, mysql_mutex_t* mutex, HASH* open_tables, JavaVM* jvm)
      : handler(hton, table_arg), jvm(jvm), cloud_mutex(mutex), cloud_open_tables(open_tables), hbase_adapter(NULL)
    {
      this->ref_length = 16;
      this->ref = new uchar[this->ref_length];
      this->initialize_adapter();
      this->rows_written = 0;
      this->rows_deleted = 0;
    }

    ~CloudHandler()
    {
      delete[] this->ref;
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
        HA_STATS_RECORDS_IS_EXACT | 
        HA_NULL_IN_KEY; // Nulls in indexed columns are allowed
    }

    ulong index_flags(uint inx, uint part, bool all_parts) const
    {
      return HA_READ_NEXT | HA_READ_ORDER | HA_READ_RANGE | HA_READ_PREV | HA_ONLY_WHOLE_INDEX;
    }

    uint max_supported_record_length() const
    {
      return HA_MAX_REC_LENGTH;
    }

    uint max_supported_keys() const
    {
      return MAX_INDEXES;
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
    ha_rows records_in_range(uint inx, key_range *min_key, key_range *max_key);
};

#endif
