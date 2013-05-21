/*
 * Copyright (C) 2013 Altamira Corporation
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


#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation        // gcc: Class implementation
#endif

#include "HoneycombHandler.h"
#include "HoneycombShare.h"
#include "Macros.h"
#include "JNISetup.h"
#include "JNICache.h"
#include "Java.h"
#include "Settings.h"
#include <cstdlib>
#include <jni.h>

#define SETTINGS_BASE "/usr/share/mysql/honeycomb/"
#define CONFIG_FILE "honeycomb.xml"
#define SCHEMA "honeycomb.xsd"
#define DEFAULT_LOG_FILE "honeycomb-c.log"
#define DEFAULT_LOG_PATH "/var/log/mysql/"

static handler *honeycomb_create_handler(handlerton *hton,
    TABLE_SHARE *table, MEM_ROOT *mem_root);
static char* honeycomb_configuration_path = NULL;

handlerton *honeycomb_hton;

mysql_mutex_t honeycomb_mutex;
static JavaVM* jvm;
static JNICache* cache;
static HASH honeycomb_open_tables;
static jobject handler_proxy_factory = NULL;

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
    HA_INPLACE_DROP_UNIQUE_INDEX_NO_READ_WRITE |
    HA_INPLACE_DROP_PK_INDEX_NO_READ_WRITE |
    HA_INPLACE_ADD_INDEX_NO_WRITE |
    HA_INPLACE_DROP_INDEX_NO_WRITE |
    HA_INPLACE_DROP_UNIQUE_INDEX_NO_WRITE |
    HA_INPLACE_DROP_PK_INDEX_NO_WRITE;
}

static jobject handler_factory(JNIEnv* env)
{
  jobject handler_proxy_local = env->CallObjectMethod(handler_proxy_factory,
      cache->handler_proxy_factory().createHandlerProxy);
  check_exceptions(env, cache, "HoneycombHandlerton::handler_factory");
  jobject handler_proxy = env->NewGlobalRef(handler_proxy_local);
  NULL_CHECK_ABORT(handler_proxy, "Out of Memory while creating global ref to HandlerProxy");
  env->DeleteLocalRef(handler_proxy_local);
  return handler_proxy;
}

bool test_directory(const char* path)
{
  if (!does_path_exist(path))
  {
    const char* missing_message = "Path %s must exist. Ensure that the full path exists and is owned by MySQL's user. %s";
    fprintf(stderr, missing_message, path, strerror(errno));
    return false;
  }

  if (!is_owned_by_mysql(path))
  {
    const char* wrong_owner_message = "Path %s must be owned by %s. Currently owner %s. %s\n";
    char owner[256], current[256];
    get_current_user_group(owner, sizeof(owner));
    get_file_user_group(path, current, sizeof(current));
    fprintf(stderr, wrong_owner_message, path, owner, current, strerror(errno));
    return false;
  }

  return true;
}

static void find_config_file(Settings& settings)
{
  const int path_count = 2;
  const char* paths[] = {
    honeycomb_configuration_path,
    SETTINGS_BASE
  };

  for (int i = 0; i < path_count; i++)
  {
    const char* path = paths[i];
    if (path == NULL)
      continue;

    Logging::info("Looking in %s for the honeycomb configuration.", path);
    char* config_buffer = format_directory_file_path(path, CONFIG_FILE);
    char* schema_buffer = format_directory_file_path(path, SCHEMA);
    bool success = settings.try_load(config_buffer, schema_buffer);

    delete[] config_buffer;
    delete[] schema_buffer;

    if (success)
    {
      Logging::info("Honeycomb configuration found in %s", path);
      return;
    }

    Logging::warn("Honeycomb configuration was not found in %s. Error trying to load files: %s", path, settings.get_errormessage());
  }
}

static bool try_setup()
{
  if (!test_directory(DEFAULT_LOG_PATH))
    return false;

  if (!Logging::try_setup_logging(DEFAULT_LOG_PATH DEFAULT_LOG_FILE))
  {
    return false;
  }

  fprintf(stderr, "Detailed logging output configured to: %s%s\n", DEFAULT_LOG_PATH, DEFAULT_LOG_FILE);
  char* cwd = getcwd(NULL, 0);
  Logging::info("Honeycomb's current directory: %s", cwd);
  free(cwd);

  Settings settings;
  find_config_file(settings);
  if (settings.has_error())
  {
    const char* error_message = settings.get_errormessage();
    Logging::fatal("Error reading the settings during setup: %s", error_message);

    return false;
  } else {
    Logging::info("Finished reading configuration settings");
  }

  if (!try_initialize_jvm(&jvm, settings, &handler_proxy_factory))
  {
	Logging::fatal("Error during JVM initialization");

    return false;
  } else {
    Logging::info("Initialized JVM");
  }

  cache = new JNICache(jvm);
  if (cache->has_error())
  {
	Logging::fatal("Error creating JNI cache");

    delete cache;
    return false;
  } else {
    Logging::info("Created JNI cache");
  }

  return true;
}

static int honeycomb_init_func(void *p)
{
  DBUG_ENTER("ha_honeycomb::honeycomb_init_func");
  if (!try_setup())
  {
    DBUG_RETURN(1);
  }

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

static handler* honeycomb_create_handler(handlerton *hton, TABLE_SHARE *table_share,
    MEM_ROOT *mem_root)
{
  JNIEnv* env;
  attach_thread(jvm, &env);
  jobject handler_proxy = handler_factory(env);
  detach_thread(jvm);
  return new (mem_root) HoneycombHandler(hton, table_share, &honeycomb_mutex,
      &honeycomb_open_tables, jvm, cache, handler_proxy);
}

struct st_mysql_storage_engine honeycomb_storage_engine=
{
  MYSQL_HANDLERTON_INTERFACE_VERSION
};

static MYSQL_SYSVAR_STR(configuration_path, honeycomb_configuration_path, 
    PLUGIN_VAR_READONLY|PLUGIN_VAR_RQCMDARG, "The path to the directory containing honeycomb.xml", NULL, NULL, NULL);

// System variables are formed by prepending the storage engine name on the front
// e.g. to set configuration_path use --honeycomb_configuration_path=<Some path>
static struct st_mysql_sys_var* honeycomb_system_variables[] = {
  MYSQL_SYSVAR(configuration_path),
  NULL
};

mysql_declare_plugin(honeycomb)
{
  MYSQL_STORAGE_ENGINE_PLUGIN,
    &honeycomb_storage_engine,
    "Honeycomb",
    "Near Infinity Corporation",
    "HBase storage engine",
    PLUGIN_LICENSE_GPL,
    honeycomb_init_func,        /* Plugin Init */
    honeycomb_done_func,        /* Plugin Deinit */
    0x0001                      /* 0.1 */,
    NULL,                       /* status variables */
    honeycomb_system_variables, /* system variables */
    NULL,                       /* config options */
    NULL,                       /* flags */
}
mysql_declare_plugin_end;
