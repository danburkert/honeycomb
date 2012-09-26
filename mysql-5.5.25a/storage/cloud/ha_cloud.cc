#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation        // gcc: Class implementation
#endif

#include "sql_priv.h"
#include "sql_class.h"           // MYSQL_HANDLERTON_INTERFACE_VERSION
#include "ha_cloud.h"
#include "probes_mysql.h"
#include "sql_plugin.h"
#include <stdlib.h>
#include "Transaction.h"

static handler *cloud_create_handler(handlerton *hton,
    TABLE_SHARE *table,
    MEM_ROOT *mem_root);

handlerton *cloud_hton;

mysql_mutex_t cloud_mutex;
static JNIEnv* env = NULL;
static JavaVM* jvm = NULL;
static HASH cloud_open_tables;

static uchar* cloud_get_key(CloudShare *share, size_t *length, my_bool not_used __attribute__((unused)))
{
  *length=share->table_path_length;
  return (uchar*) share->path_to_table;
}

#ifdef HAVE_PSI_INTERFACE
static PSI_mutex_key ex_key_mutex_cloud, ex_key_mutex_cloud_SHARE_mutex;

static PSI_mutex_info all_cloud_mutexes[]=
{
  { &ex_key_mutex_cloud, "cloud", PSI_FLAG_GLOBAL},
  { &ex_key_mutex_cloud_SHARE_mutex, "cloud_SHARE::mutex", 0}
};

static void init_cloud_psi_keys()
{
  const char* category= "cloud";
  int count;

  if (PSI_server == NULL)
    return;

  count= array_elements(all_cloud_mutexes);
  PSI_server->register_mutex(category, all_cloud_mutexes, count);
}
#endif

static uint cloud_alter_table_flags(uint flags)
{
  return HA_INPLACE_ADD_INDEX_NO_READ_WRITE |
    HA_INPLACE_DROP_INDEX_NO_READ_WRITE |
    HA_INPLACE_ADD_UNIQUE_INDEX_NO_READ_WRITE |
    HA_INPLACE_DROP_UNIQUE_INDEX_NO_READ_WRITE |
    HA_INPLACE_ADD_PK_INDEX_NO_READ_WRITE |
    HA_INPLACE_DROP_PK_INDEX_NO_READ_WRITE |
    HA_INPLACE_ADD_INDEX_NO_WRITE |
    HA_INPLACE_DROP_INDEX_NO_WRITE |
    HA_INPLACE_ADD_UNIQUE_INDEX_NO_WRITE |
    HA_INPLACE_DROP_UNIQUE_INDEX_NO_WRITE |
    HA_INPLACE_ADD_PK_INDEX_NO_WRITE |
    HA_INPLACE_DROP_PK_INDEX_NO_WRITE;
}

static int cloud_init_func(void *p)
{
  DBUG_ENTER("ha_cloud::cloud_init_func");

#ifdef HAVE_PSI_INTERFACE
  init_cloud_psi_keys();
#endif

  cloud_hton = (handlerton *)p;
  mysql_mutex_init(ex_key_mutex_cloud, &cloud_mutex, MY_MUTEX_INIT_FAST);
  (void) my_hash_init(&cloud_open_tables,system_charset_info,32,0,0,
      (my_hash_get_key) cloud_get_key,0,0);

  cloud_hton->state = SHOW_OPTION_YES;
  cloud_hton->create = cloud_create_handler;
  cloud_hton->flags = HTON_TEMPORARY_NOT_SUPPORTED;
  cloud_hton->alter_table_flags = cloud_alter_table_flags;

  // Commit and rollback must be implemented to have transaction support
  cloud_hton->commit = cloud_commit;
  cloud_hton->rollback = cloud_rollback;

  // Must implement these following functions/members to have support for savepoints
  cloud_hton->savepoint_set = 0;
  cloud_hton->savepoint_rollback = 0;
  cloud_hton->savepoint_release = 0;
  cloud_hton->savepoint_offset = 0;

  Logging::setup_logging(NULL);
  create_or_find_jvm(&jvm);

  DBUG_RETURN(0);
}

static int cloud_done_func(void *p)
{
  int error = 0;
  DBUG_ENTER("cloud_done_func");

  if (cloud_open_tables.records)
  {
    error= 1;
  }

  Logging::close_logging();
  my_hash_free(&cloud_open_tables);
  mysql_mutex_destroy(&cloud_mutex);
  DBUG_RETURN(error);
}

/**
  @brief
  Free lock controls. We call this whenever we close a table. If the table had
  the last reference to the share, then we free memory associated with it.
  */

static int free_share(CloudShare *share)
{
  mysql_mutex_lock(&cloud_mutex);
  if (!--share->use_count)
  {
    my_hash_delete(&cloud_open_tables, (uchar*) share);
    thr_lock_delete(&share->lock);
    mysql_mutex_destroy(&share->mutex);
    my_free(share);
  }

  mysql_mutex_unlock(&cloud_mutex);
  return 0;
}

static handler* cloud_create_handler(handlerton *hton, TABLE_SHARE *table, MEM_ROOT *mem_root)
{
  return new (mem_root) CloudHandler(hton, table, &cloud_mutex, &cloud_open_tables, jvm);
}

struct st_mysql_storage_engine cloud_storage_engine=
{
  MYSQL_HANDLERTON_INTERFACE_VERSION 
};

mysql_declare_plugin(cloud)
{
  MYSQL_STORAGE_ENGINE_PLUGIN,
    &cloud_storage_engine,
    "cloud",
    "Near Infinity Corporation",
    "HBase storage engine",
    PLUGIN_LICENSE_GPL,
    cloud_init_func,                            /* Plugin Init */
    cloud_done_func,                            /* Plugin Deinit */
    0x0001 /* 0.1 */,
    NULL,                                  /* status variables */
    NULL,                     /* system variables */
    NULL,                                         /* config options */
    NULL,                                            /* flags */
}
mysql_declare_plugin_end;
