#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation        // gcc: Class implementation
#endif

#include "sql_priv.h"
#include "sql_class.h"           // MYSQL_HANDLERTON_INTERFACE_VERSION
#include "probes_mysql.h"
#include "sql_plugin.h"
#include <stdlib.h>

#include "HoneycombHandler.h"
#include "HoneycombShare.h"
#include "Util.h"
#include "Macros.h"
#include "JNISetup.h"
#include "Logging.h"
#include "JNICache.h"
#include "Row.h"
#include "TableSchema.h"
#include "IndexSchema.h"
#include "ColumnSchema.h"

static handler *honeycomb_create_handler(handlerton *hton,
    TABLE_SHARE *table, MEM_ROOT *mem_root);

handlerton *honeycomb_hton;

mysql_mutex_t honeycomb_mutex;
static JavaVM* jvm;
static JNICache* cache;
static HASH honeycomb_open_tables;

static uchar* honeycomb_get_key(HoneycombShare *share, size_t *length,
    my_bool not_used __attribute__((unused)))
{
  *length=share->table_path_length;
  return (uchar*) share->path_to_table;
}

#ifdef HAVE_PSI_INTERFACE
static PSI_mutex_key ex_key_mutex_honeycomb, ex_key_mutex_honeycomb_SHARE_mutex;

static PSI_mutex_info all_honeycomb_mutexes[]=
{
  { &ex_key_mutex_honeycomb, "honeycomb", PSI_FLAG_GLOBAL},
  { &ex_key_mutex_honeycomb_SHARE_mutex, "honeycomb_SHARE::mutex", 0}
};

static void init_honeycomb_psi_keys()
{
  const char* category= "honeycomb";
  int count;

  if (PSI_server == NULL)
    return;

  count= array_elements(all_honeycomb_mutexes);
  PSI_server->register_mutex(category, all_honeycomb_mutexes, count);
}
#endif

static uint honeycomb_alter_table_flags(uint flags)
{
  return
    HA_INPLACE_ADD_INDEX_NO_READ_WRITE |
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
static jobject handler_proxy_factory;
static jobject handler_factory(JNIEnv* env)
{
  jobject handler_proxy_local = env->CallObjectMethod(handler_proxy_factory,
      cache->handler_proxy_factory().createHandlerProxy);
  EXCEPTION_CHECK("ha_honeycomb::handler_factory", "createHandlerProxy");
  jobject handler_proxy = env->NewGlobalRef(handler_proxy_local);
  NULL_CHECK_ABORT(handler_proxy, "Out of Memory while creating global ref to HandlerProxy");
  env->DeleteLocalRef(handler_proxy_local);
  return handler_proxy;
}

static int honeycomb_init_func(void *p)
{
  DBUG_ENTER("ha_honeycomb::honeycomb_init_func");
  Logging::setup_logging(NULL);

#ifdef HAVE_PSI_INTERFACE
  init_honeycomb_psi_keys();
#endif

  honeycomb_hton = (handlerton *)p;
  mysql_mutex_init(ex_key_mutex_honeycomb, &honeycomb_mutex, MY_MUTEX_INIT_FAST);
  (void) my_hash_init(&honeycomb_open_tables,system_charset_info,32,0,0,
      (my_hash_get_key) honeycomb_get_key,0,0);

  honeycomb_hton->state = SHOW_OPTION_YES;
  honeycomb_hton->create = honeycomb_create_handler;
  honeycomb_hton->flags = HTON_TEMPORARY_NOT_SUPPORTED;
  honeycomb_hton->alter_table_flags = honeycomb_alter_table_flags;

  handler_proxy_factory = initialize_jvm(jvm);
  cache = new JNICache(jvm);

  DBUG_RETURN(0);
}

static int honeycomb_done_func(void *p)
{
  int error = 0;
  DBUG_ENTER("honeycomb_done_func");

  if (honeycomb_open_tables.records)
  {
    error= 1;
  }
  delete cache;
  Logging::close_logging();
  my_hash_free(&honeycomb_open_tables);
  mysql_mutex_destroy(&honeycomb_mutex);
  DBUG_RETURN(error);
}

static handler* honeycomb_create_handler(handlerton *hton, TABLE_SHARE *table,
    MEM_ROOT *mem_root)
{
  JNIEnv* env;
  attach_thread(jvm, env);
  jobject handler_proxy = handler_factory(env);
  detach_thread(jvm);
  return new (mem_root) HoneycombHandler(hton, table, &honeycomb_mutex,
      &honeycomb_open_tables, jvm, cache, handler_proxy);
}

struct st_mysql_storage_engine honeycomb_storage_engine=
{
  MYSQL_HANDLERTON_INTERFACE_VERSION
};

mysql_declare_plugin(honeycomb)
{
  MYSQL_STORAGE_ENGINE_PLUGIN,
    &honeycomb_storage_engine,
    "Honeycomb",
    "Near Infinity Corporation",
    "HBase storage engine",
    PLUGIN_LICENSE_GPL,
    honeycomb_init_func, /* Plugin Init */
    honeycomb_done_func, /* Plugin Deinit */
    0x0001               /* 0.1 */,
    NULL,                /* status variables */
    NULL,                /* system variables */
    NULL,                /* config options */
    NULL,                /* flags */
}
mysql_declare_plugin_end;
