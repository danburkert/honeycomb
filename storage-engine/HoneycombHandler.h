/*
 * Copyright (C) 2013 Near Infinity Corporation
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


#ifndef HONEYCOMB_HANDLER_H
#define HONEYCOMB_HANDLER_H

#ifdef USE_PRAGMA_INTERFACE
#pragma Interface               /* gcc class implementation */
#endif

#include "Util.h"
#include "QueryKey.h"

#include "my_global.h"          /* ulonglong */
#include "thr_lock.h"           /* THR_LOCK, THR_LOCK_DATA */
#include "handler.h"            /* handler */
#include "my_base.h"            /* ha_rows */

class JNICache;
class Row;
class ColumnSchema;
class IndexSchema;

struct st_honeycomb_share;
typedef st_honeycomb_share HoneycombShare;

struct JavaVM_;
typedef JavaVM_ JavaVM;

struct JNIEnv_;
typedef JNIEnv_ JNIEnv;

class _jobject;
typedef _jobject *jobject;

class _jbyteArray;
typedef _jbyteArray *jbyteArray;


/**
 * @brief The primary interface between the storage engine and MySQL. 
 * The class is composed of ddl, dml and table cursor methods.
 */
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

    bool is_integral_field(enum_field_types field_type);
    bool is_date_or_time_field(enum_field_types field_type);
    bool is_floating_point_field(enum_field_types field_type);
    bool is_decimal_field(enum_field_types field_type);
    bool is_byte_field(enum_field_types field_type);
    bool is_unsupported_field(enum_field_types field_type);

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
    /* HoneycombHandler */
    /**
     * @brief Construct a new HoneycombHandler
     *
     * @param hton The storage engine's handlerton 
     * @param table_share Shared table data
     * @param mutex Table lock?
     * @param open_tables Hash of open MySQL tables
     * @param jvm Java VM
     * @param cache Cache of JNI class objects
     * @param handler_proxy Constructed HandlerProxy
     */
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
    int analyze(THD* thd, HA_CHECK_OPT* check_opt);
    ha_rows estimate_rows_upper_bound();
    const char *table_type() const;
    const char *index_type(uint inx);
    uint alter_table_flags(uint flags);
    ulonglong table_flags() const;
    ulong index_flags(uint inx, uint part, bool all_parts) const;
    uint max_supported_record_length() const;
    uint max_supported_keys() const;
    uint max_supported_key_length() const;
    uint max_supported_key_part_length() const;
    uint max_supported_key_parts() const;
    virtual double scan_time();
    virtual double read_time(uint index, uint ranges, ha_rows rows);
    virtual int final_add_index(handler_add_index *add, bool commit);
    virtual int final_drop_index(TABLE *table_arg);

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
