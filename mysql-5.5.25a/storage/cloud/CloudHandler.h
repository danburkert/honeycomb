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

class CloudHandler : public handler
{
private:
	THR_LOCK_DATA lock;      	///< MySQL lockCloudShare;
	CloudShare *share;    		///< Shared lock info
    CloudShare *get_share(const char *table_name, TABLE *table)
    {
        CloudShare *share;
        char meta_file_name[FN_REFLEN];
        MY_STAT file_stat;                /* Stat information for the data file */
        char *tmp_name;
        uint length;

        mysql_mutex_lock(&cloud_mutex);
        length=(uint) strlen(table_name);

        /*
        If share is not present in the hash, create a new share and
        initialize its members.
        */
        if (!(share=(CloudShare*) my_hash_search(&cloud_open_tables,
                                               (uchar*) table_name,
                                               length)))
        {
          if (!my_multi_malloc(MYF(MY_WME | MY_ZEROFILL),
                               &share, sizeof(*share),
                               &tmp_name, length+1,
                               NullS))
          {
            mysql_mutex_unlock(&cloud_mutex);
            return NULL;
          }
        }

        share->use_count= 0;
        share->table_name_length= length;
        share->table_name= tmp_name;
        share->crashed= FALSE;
        share->rows_recorded= 0;
        share->data_file_version= 0;
        strmov(share->table_name, table_name);
        fn_format(share->data_file_name, table_name, "", NullS,
                  MY_REPLACE_EXT|MY_UNPACK_FILENAME);

        if (my_hash_insert(&cloud_open_tables, (uchar*) share))
          goto error;
        thr_lock_init(&share->lock);

        share->use_count++;
        mysql_mutex_unlock(&cloud_mutex);

        return share;

        error:
        mysql_mutex_unlock(&cloud_mutex);
        my_free(share);

        return NULL;
    }

    public:
      CloudHandler(handlerton *hton, TABLE_SHARE *table_arg) : handler(hton, table_arg)
      {}

      ~CloudHandler()
      {
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
      int update_row(const uchar *old_data, uchar *new_data);
      int write_row(uchar *buf);
      int delete_row(const uchar *buf);
      static int free_share(CloudShare *share);
    };

#endif
