#ifndef HONEYCOMB_HANDLER_H
#define HONEYCOMB_HANDLER_H

#ifdef USE_PRAGMA_INTERFACE
#pragma Interface               /* gcc class implementation */
#endif

#include "HoneycombShare.h"
#include "Util.h"

#include "my_global.h"          /* ulonglong */
#include "thr_lock.h"           /* THR_LOCK, THR_LOCK_DATA */
#include "handler.h"            /* handler */
#include "my_base.h"            /* ha_rows */
#include <jni.h>
#include "probes_mysql.h"

class JNICache;

class HoneycombHandler : public handler
{
  private:
    THR_LOCK_DATA lock;     ///< MySQL lockHoneycombShare;
    HoneycombShare *share;  ///< Sharedclass lock info
    mysql_mutex_t* honeycomb_mutex;
    HASH* honeycomb_open_tables;
    bool performing_scan;
    HoneycombShare *get_share(const char *table_name, TABLE *table);
    uint32 max_row_length();

    long long curr_scan_id, curr_write_id;
    ulonglong rows_written;

    uint failed_key_index;

    // HBase JNI Adapter:
    JNIEnv* env;
    JavaVM* jvm;
    JNICache* cache;

    jstring table_name();
    const char* java_to_string(jstring str);
    jstring string_to_java_string(const char *string);
    int java_to_sql(uchar *buf, jobject row_map);
    jobject sql_to_java();
    int delete_all_rows();
    int delete_table(const char *name);
    void drop_table(const char *name);
    int truncate();
    bool is_key_null(const uchar *key);
    void store_uuid_ref(jobject row);
    void bytes_to_long(const uchar* buff, unsigned int buff_length, bool is_signed, uchar* long_buff);
    int read_index_row(jobject index_row, uchar* buf);
    int get_index_row(jfieldID field_id, uchar* buf);
    int get_next_index_row(uchar* buf);
    void flush_writes();
    void end_scan();
    bool check_for_renamed_column(const TABLE*  table, const char* col_name);
    bool field_has_unique_index(Field *field);
    jbyteArray find_duplicate_column_values(char* columns);
    bool row_has_duplicate_values(jobject value_map, jobject changedColumns);
    int get_failed_key_index(const char *key_name);
    void store_field_value(Field *field, char *key, int length);
    jobject create_multipart_keys(TABLE* table_arg);
    jobject create_multipart_key(KEY* key, KEY_PART_INFO* key_part, KEY_PART_INFO* key_part_end, uint key_parts);
    char* index_name(KEY_PART_INFO* key_part, KEY_PART_INFO* key_part_end, uint key_parts);
    char* index_name(TABLE* table, uint key);
    jobject create_key_value_list(int index, uint* key_sizes, uchar** key_copies, const char** key_names, jboolean* key_null_bits, jboolean* key_is_null);
    bool is_field_nullable(jstring table_name, const char* field_name);
    bool is_allowed_column(Field* field, int* error_number);
    int retrieve_value_from_index(uchar* buf);
    int write_row(uchar* buf, jobject updated_fields);
    void collect_changed_fields(jobject updated_fields, const uchar* old_row, uchar* new_row);
    void terminate_scan();

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

    bool is_floating_point_field(int field_type)
    {
      return (field_type == MYSQL_TYPE_FLOAT || field_type == MYSQL_TYPE_DOUBLE);
    }

    bool is_decimal_field(int field_type)
    {
      return (field_type == MYSQL_TYPE_DECIMAL || field_type == MYSQL_TYPE_NEWDECIMAL);
    }

    bool is_byte_field(int field_type)
    {
      return (field_type == MYSQL_TYPE_VARCHAR
    || field_type == MYSQL_TYPE_VAR_STRING
    || field_type == MYSQL_TYPE_STRING
    || field_type == MYSQL_TYPE_BLOB
    || field_type == MYSQL_TYPE_TINY_BLOB
    || field_type == MYSQL_TYPE_MEDIUM_BLOB
    || field_type == MYSQL_TYPE_LONG_BLOB);
    }

    bool is_unsupported_field(int field_type)
    {
      return (field_type == MYSQL_TYPE_NULL
      || field_type == MYSQL_TYPE_BIT
      || field_type == MYSQL_TYPE_SET
      || field_type == MYSQL_TYPE_GEOMETRY);
    }

    /* Index methods */
    int index_init(uint idx, bool sorted);
    int index_end();
    int index_next(uchar *buf);
    int index_prev(uchar *buf);
    int index_first(uchar *buf);
    int index_last(uchar *buf);

    void set_autoinc_counter(jlong new_value, jboolean is_truncate);
    void release_auto_increment();

  public:
    HoneycombHandler(handlerton *hton, TABLE_SHARE *table_arg,
        mysql_mutex_t* mutex, HASH* open_tables, JavaVM* jvm, JNICache* cache);
    ~HoneycombHandler();

    const char *table_type() const
    {
      return "Honeycomb";
    }

    const char *index_type(uint inx)
    {
      return "HASH";
    }

    uint alter_table_flags(uint flags)
    {
      if (ht->alter_table_flags)
      {
        return ht->alter_table_flags(flags);
      }

      return 0;
    }

    ulonglong table_flags() const
    {
      return HA_FAST_KEY_READ |
        HA_BINLOG_STMT_CAPABLE |
        HA_REC_NOT_IN_SEQ |
        HA_NO_TRANSACTIONS |
        HA_NULL_IN_KEY | // Nulls in indexed columns are allowed
        HA_TABLE_SCAN_ON_INDEX;
    }

    ulong index_flags(uint inx, uint part, bool all_parts) const
    {
      return HA_READ_NEXT | HA_READ_ORDER | HA_READ_RANGE
          | HA_READ_PREV | HA_ONLY_WHOLE_INDEX | HA_KEYREAD_ONLY;
    }

    uint max_supported_record_length() const
    {
      return HA_MAX_REC_LENGTH;
    }

    uint max_supported_keys() const
    {
      return MAX_INDEXES;
    }

    uint max_supported_key_length() const
    {
      return UINT_MAX;
    }

    uint max_supported_key_part_length() const
    {
      return UINT_MAX;
    }

    uint max_supported_key_parts() const
    {
      return 4;
    }

    virtual double scan_time()
    {
      return (double) (stats.records+stats.deleted) / 20.0+10;
    }

    virtual double read_time(uint, uint, ha_rows rows)
    {
      return (double) rows /  20.0+1;
    }

    virtual int final_add_index(handler_add_index *add, bool commit)
    {
      return 0;
    }

    virtual int final_drop_index(TABLE *table_arg)
    {
      return 0;
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
    void update_create_info(HA_CREATE_INFO* create_info);
    int extra(enum ha_extra_function operation);
    int update_row(const uchar *old_data, uchar *new_data);
    int write_row(uchar *buf);
    int delete_row(const uchar *buf);
    int free_share(HoneycombShare *share);
    int rnd_end();
    ha_rows records_in_range(uint inx, key_range *min_key, key_range *max_key);
    int analyze(THD* thd, HA_CHECK_OPT* check_opt);
    ha_rows estimate_rows_upper_bound();
    bool check_if_incompatible_data(HA_CREATE_INFO *create_info, uint table_changes);
    int rename_table(const char *from, const char *to);
    void get_auto_increment(ulonglong offset, ulonglong increment, ulonglong nb_desired_values, ulonglong *first_value, ulonglong *nb_reserved_values);
    int index_read_map(uchar * buf, const uchar * key, key_part_map keypart_map, enum ha_rkey_function find_flag);
    int add_index(TABLE *table_arg, KEY *key_info, uint num_of_keys, handler_add_index **add);
    int prepare_drop_index(TABLE *table_arg, uint *key_num, uint num_of_keys);
};

#endif
