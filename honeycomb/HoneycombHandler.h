#ifndef HONEYCOMB_HANDLER_H
#define HONEYCOMB_HANDLER_H

#ifdef USE_PRAGMA_INTERFACE
#pragma Interface               /* gcc class implementation */
#endif

#include "HoneycombShare.h"
#include "Util.h"
#include "QueryKey.h"

#include "my_global.h"          /* ulonglong */
#include "thr_lock.h"           /* THR_LOCK, THR_LOCK_DATA */
#include "handler.h"            /* handler */
#include "my_base.h"            /* ha_rows */
#include <jni.h>
#include "probes_mysql.h"

class JNICache;
class Row;
class ColumnSchema;
class IndexSchema;


class HoneycombHandler : public handler
{
  private:
    THR_LOCK_DATA lock;     ///< MySQL lockHoneycombShare;
    HoneycombShare *share;  ///< Sharedclass lock info
    mysql_mutex_t* honeycomb_mutex;
    HASH* honeycomb_open_tables;
    HoneycombShare *get_share(const char *table_name, TABLE *table);

    ulonglong rows_written;
    uint failed_key_index;

    // JNI State:
    JNIEnv* env;
    JavaVM* jvm;
    JNICache* cache;
    jobject handler_proxy;
    Row* row;

    bool is_integral_field(enum_field_types field_type)
    {
      return (field_type == MYSQL_TYPE_LONG
          || field_type == MYSQL_TYPE_SHORT
          || field_type == MYSQL_TYPE_TINY
          || field_type == MYSQL_TYPE_LONGLONG
          || field_type == MYSQL_TYPE_INT24
          || field_type == MYSQL_TYPE_ENUM
          || field_type == MYSQL_TYPE_YEAR);
    }

    bool is_date_or_time_field(enum_field_types field_type)
    {
      return (field_type == MYSQL_TYPE_DATE
          || field_type == MYSQL_TYPE_DATETIME
          || field_type == MYSQL_TYPE_TIME
          || field_type == MYSQL_TYPE_TIMESTAMP
          || field_type == MYSQL_TYPE_NEWDATE);
    }

    bool is_floating_point_field(enum_field_types field_type)
    {
      return (field_type == MYSQL_TYPE_FLOAT || field_type == MYSQL_TYPE_DOUBLE);
    }

    bool is_decimal_field(enum_field_types field_type)
    {
      return (field_type == MYSQL_TYPE_DECIMAL || field_type == MYSQL_TYPE_NEWDECIMAL);
    }

    bool is_byte_field(enum_field_types field_type)
    {
      return (field_type == MYSQL_TYPE_VARCHAR
    || field_type == MYSQL_TYPE_VAR_STRING
    || field_type == MYSQL_TYPE_STRING
    || field_type == MYSQL_TYPE_BLOB
    || field_type == MYSQL_TYPE_TINY_BLOB
    || field_type == MYSQL_TYPE_MEDIUM_BLOB
    || field_type == MYSQL_TYPE_LONG_BLOB);
    }

    bool is_unsupported_field(enum_field_types field_type)
    {
      return (field_type == MYSQL_TYPE_NULL
      || field_type == MYSQL_TYPE_BIT
      || field_type == MYSQL_TYPE_SET
      || field_type == MYSQL_TYPE_GEOMETRY);
    }

    /* HoneycombHandler helper methods */
    void store_uuid_ref(Row* row);

    /* Query helper methods */
    int start_index_scan(Serializable& index_key, uchar* buf);
    int read_row(uchar* buf);
    int get_next_row(uchar* buf);
    int read_bytes_into_mysql(jbyteArray row_bytes, uchar* buf);
    int full_index_scan(uchar* buf, QueryKey::QueryType query);
    int retrieve_value_from_index(uchar* buf);
    int unpack_row(uchar *buf, Row& row);
    void store_field_value(Field *field, const char* val, int length);

    /* DDL helper methods */
    int pack_column_schema(ColumnSchema* schema, Field* field);
    int pack_index_schema(IndexSchema* schema, KEY* key);
    int init_table_share(TABLE_SHARE* table_share, const char* path);
    bool is_allowed_column(Field* field, int* error_number);
    bool check_column_being_renamed(const TABLE*  table);

    /* IUD helper methods*/
    bool violates_uniqueness(jbyteArray serialized_row);
    int pack_row(uchar *buf, TABLE* table, Row& row);


  public:
    const char *table_type() const
    {
      return "Honeycomb";
    }

    const char *index_type(uint inx)
    {
      return "BTREE";
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
        | HA_READ_PREV | HA_KEYREAD_ONLY;
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
      return MAX_REF_PARTS;
    }

    virtual double scan_time()
    {
      return ((stats.records+stats.deleted) / 20.0) + 10;
    }

    virtual double read_time(uint, uint, ha_rows rows)
    {
      return (rows / 20.0) + 1;
    }

    virtual int final_add_index(handler_add_index *add, bool commit)
    {
      return 0;
    }

    virtual int final_drop_index(TABLE *table_arg)
    {
      return 0;
    }

    /* HoneycombHandler */
    HoneycombHandler(handlerton *hton, TABLE_SHARE *table_share,
        mysql_mutex_t* mutex, HASH* open_tables, JavaVM* jvm, JNICache* cache, jobject handler_proxy);
    ~HoneycombHandler();
    const char **bas_ext() const;
    int open(const char *name, int mode, uint test_if_locked);    // required
    int close(void);                                              // required
    void position(const uchar *record);                           ///< required
    int info(uint);                                               ///< required
    int external_lock(THD *thd, int lock_type);                   ///< required
    void get_auto_increment(ulonglong offset, ulonglong increment, ulonglong nb_desired_values, ulonglong *first_value, ulonglong *nb_reserved_values);
    void release_auto_increment();
    int flush();
    THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type);     ///< required
    int extra(enum ha_extra_function operation);
    int free_share(HoneycombShare *share);
    ha_rows records_in_range(uint inx, key_range *min_key, key_range *max_key);
    int analyze(THD* thd, HA_CHECK_OPT* check_opt);
    ha_rows estimate_rows_upper_bound();

    /* Query */
    int index_init(uint idx, bool sorted);
    int index_read_map(uchar * buf, const uchar * key, key_part_map keypart_map, enum ha_rkey_function find_flag);
    int index_read_last_map(uchar * buf, const uchar * key, key_part_map keypart_map);
    int index_next(uchar *buf);
    int index_prev(uchar *buf);
    int index_first(uchar *buf);
    int index_last(uchar *buf);
    int index_end();

    int rnd_init(bool scan);                                      //required
    int rnd_next(uchar *buf);                                     ///< required
    int rnd_pos(uchar *buf, uchar *pos);                          ///< required
    int rnd_end();

    /* DDL */
    int create(const char *name, TABLE *form, HA_CREATE_INFO *create_info); ///< required
    int delete_table(const char *name);
    int rename_table(const char *from, const char *to);
    bool check_if_incompatible_data(HA_CREATE_INFO *create_info, uint table_changes);
    int prepare_drop_index(TABLE *table_arg, uint *key_num, uint num_of_keys);
    int add_index(TABLE *table_arg, KEY *key_info, uint num_of_keys, handler_add_index **add);
    void update_create_info(HA_CREATE_INFO* create_info);

    /* IUD */
    int write_row(uchar *buf);
    int update_row(const uchar *old_data, uchar *new_data);
    int delete_row(const uchar *buf);
    int delete_all_rows();
    int truncate();
};

#endif
