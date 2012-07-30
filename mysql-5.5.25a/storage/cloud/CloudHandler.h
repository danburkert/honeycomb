#ifndef CLOUD_HANDLER_H
#define CLOUD_HANDLER_H

#ifdef USE_PRAGMA_INTERFACE
#pragma interface			/* gcc class implementation */
#endif

#include "my_global.h"                   /* ulonglong */
#include "thr_lock.h"                    /* THR_LOCK, THR_LOCK_DATA */
#include "handler.h"                     /* handler */
#include "my_base.h"                     /* ha_rows */
#include "ha_cloud.h"

class CloudHandler : public handler
{
private:
	THR_LOCK_DATA lock;      	///< MySQL lockCloudShare;
	CloudShare *share;    		///< Shared lock info

public:
  CloudHandler(handlerton *hton, TABLE_SHARE *table_arg);
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
  static int free_share(CloudShare *share);
};

#endif
