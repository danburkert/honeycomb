#ifdef USE_PRAGMA_INTERFACE
#pragma interface			/* gcc class implementation */
#endif

#include "thr_lock.h"                    /* THR_LOCK, THR_LOCK_DATA */

typedef struct st_cloud_share {
  char *table_name;
  uint table_name_length,use_count;
  mysql_mutex_t mutex;
  THR_LOCK lock;
} CloudShare;
