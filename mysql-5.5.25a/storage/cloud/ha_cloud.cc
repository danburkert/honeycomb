/* Copyright (c) 2004, 2011, Oracle and/or its affiliates. All rights reserved.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/**
  @file ha_cloud.cc

  @brief
  The ha_cloud engine is a cloudbed storage engine for cloud purposes only;
  it does nothing at this point. Its purpose is to provide a source
  code illustration of how to begin writing new storage engines; see also
  /storage/cloud/ha_cloud.h.

  @details
  ha_cloud will let you create/open/delete tables, but
  nothing further (for cloud, indexes are not supported nor can data
  be stored in the table). Use this cloud as a template for
  implementing the same functionality in your own storage engine. You
  can enable the cloud storage engine in your build by doing the
  following during your build process:<br> ./configure
  --with-cloud-storage-engine

  Once this is done, MySQL will let you create tables with:<br>
  CREATE TABLE <table name> (...) ENGINE=cloud;

  The cloud storage engine is set up to use table locks. It
  implements an cloud "SHARE" that is inserted into a hash by table
  name. You can use this to store information of state that any
  cloud handler object will be able to see when it is using that
  table.

  Please read the object definition in ha_cloud.h before reading the rest
  of this file.

  @note
  When you create an cloud table, the MySQL Server creates a table .frm
  (format) file in the database directory, using the table name as the file
  name as is customary with MySQL. No other files are created. To get an idea
  of what occurs, here is an cloud select that would do a scan of an entire
  table:

  @code
  ha_cloud::store_lock
  ha_cloud::external_lock
  ha_cloud::info
  ha_cloud::rnd_init
  ha_cloud::extra
  ENUM HA_EXTRA_CACHE        Cache record in HA_rrnd()
  ha_cloud::rnd_next
  ha_cloud::rnd_next
  ha_cloud::rnd_next
  ha_cloud::rnd_next
  ha_cloud::rnd_next
  ha_cloud::rnd_next
  ha_cloud::rnd_next
  ha_cloud::rnd_next
  ha_cloud::rnd_next
  ha_cloud::extra
  ENUM HA_EXTRA_NO_CACHE     End caching of records (def)
  ha_cloud::external_lock
  ha_cloud::extra
  ENUM HA_EXTRA_RESET        Reset database to after open
  @endcode

  Here you see that the cloud storage engine has 9 rows called before
  rnd_next signals that it has reached the end of its data. Also note that
  the table in question was already opened; had it not been open, a call to
  ha_cloud::open() would also have been necessary. Calls to
  ha_cloud::extra() are hints as to what will be occuring to the request.

  A Longer Example can be found called the "Skeleton Engine" which can be 
  found on TangentOrg. It has both an engine and a full build environment
  for building a pluggable storage engine.

  Happy coding!<br>
    -Brian
*/

#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation        // gcc: Class implementation
#endif

#include "sql_priv.h"
#include "sql_class.h"           // MYSQL_HANDLERTON_INTERFACE_VERSION
#include "ha_cloud.h"
#include "probes_mysql.h"
#include "sql_plugin.h"
#include "CloudHandler.h"

static handler *cloud_create_handler(handlerton *hton,
                                       TABLE_SHARE *table, 
                                       MEM_ROOT *mem_root);

handlerton *cloud_hton;

/* Interface to mysqld, to check system tables supported by SE */
static const char* cloud_system_database();
static bool cloud_is_supported_system_table(const char *db,
                                      const char *table_name,
                                      bool is_sql_layer_system_table);

/* Variables for cloud share methods */

/* 
   Hash used to track the number of open tables; variable for cloud share
   methods
*/
static HASH cloud_open_tables;

/* The mutex used to init the hash; variable for cloud share methods */
mysql_mutex_t cloud_mutex;

/**
  @brief
  Function we use in the creation of our hash to get key.
*/

static uchar* cloud_get_key(CloudShare *share, size_t *length, my_bool not_used __attribute__((unused)))
{
  *length=share->table_name_length;
  return (uchar*) share->table_name;
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


static int cloud_init_func(void *p)
{
  DBUG_ENTER("cloud_init_func");

#ifdef HAVE_PSI_INTERFACE
  init_cloud_psi_keys();
#endif

  cloud_hton= (handlerton *)p;
  mysql_mutex_init(ex_key_mutex_cloud, &cloud_mutex, MY_MUTEX_INIT_FAST);
  (void) my_hash_init(&cloud_open_tables,system_charset_info,32,0,0,
                      (my_hash_get_key) cloud_get_key,0,0);

  cloud_hton->state=   SHOW_OPTION_YES;
  cloud_hton->create=  cloud_create_handler;
  cloud_hton->flags=   HTON_CAN_RECREATE;
  cloud_hton->system_database=   cloud_system_database;
  cloud_hton->is_supported_system_table= cloud_is_supported_system_table;

  DBUG_RETURN(0);
}


static int cloud_done_func(void *p)
{
  int error= 0;
  DBUG_ENTER("cloud_done_func");

  if (cloud_open_tables.records)
    error= 1;
  my_hash_free(&cloud_open_tables);
  mysql_mutex_destroy(&cloud_mutex);

  DBUG_RETURN(error);
}

/**
  @brief
  Example of simple lock controls. The "share" it creates is a
  structure we will pass to each cloud handler. Do you have to have
  one of these? Well, you have pieces that are used for locking, and
  they are needed to function.
*/


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
  return new (mem_root) CloudHandler(hton, table);
}

/*
  Following handler function provides access to
  system database specific to SE. This interface
  is optional, so every SE need not implement it.
*/
const char* ha_cloud_system_database= NULL;
const char* cloud_system_database()
{
  return ha_cloud_system_database;
}

/*
  List of all system tables specific to the SE.
  Array element would look like below,
     { "<database_name>", "<system table name>" },
  The last element MUST be,
     { (const char*)NULL, (const char*)NULL }

  This array is optional, so every SE need not implement it.
*/
static st_system_tablename ha_cloud_system_tables[]= {
  {(const char*)NULL, (const char*)NULL}
};

/**
  @brief Check if the given db.tablename is a system table for this SE.

  @param db                         Database name to check.
  @param table_name                 table name to check.
  @param is_sql_layer_system_table  if the supplied db.table_name is a SQL
                                    layer system table.

  @return
    @retval TRUE   Given db.table_name is supported system table.
    @retval FALSE  Given db.table_name is not a supported system table.
*/
static bool cloud_is_supported_system_table(const char *db,
                                              const char *table_name,
                                              bool is_sql_layer_system_table)
{
  st_system_tablename *systab;

  // Does this SE support "ALL" SQL layer system tables ?
  if (is_sql_layer_system_table)
    return false;

  // Check if this is SE layer system tables
  systab= ha_cloud_system_tables;
  while (systab && systab->db)
  {
    if (systab->db == db &&
        strcmp(systab->tablename, table_name) == 0)
      return true;
    systab++;
  }

  return false;
}

struct st_mysql_storage_engine cloud_storage_engine=
{ MYSQL_HANDLERTON_INTERFACE_VERSION };

static ulong srv_enum_var= 0;
static ulong srv_ulong_var= 0;

const char *enum_var_names[]=
{
  "e1", "e2", NullS
};

TYPELIB enum_var_typelib=
{
  array_elements(enum_var_names) - 1, "enum_var_typelib",
  enum_var_names, NULL
};

static MYSQL_SYSVAR_ENUM(
  enum_var,                       // name
  srv_enum_var,                   // varname
  PLUGIN_VAR_RQCMDARG,            // opt
  "Sample ENUM system variable.", // comment
  NULL,                           // check
  NULL,                           // update
  0,                              // def
  &enum_var_typelib);             // typelib

static MYSQL_SYSVAR_ULONG(
  ulong_var,
  srv_ulong_var,
  PLUGIN_VAR_RQCMDARG,
  "0..1000",
  NULL,
  NULL,
  8,
  0,
  1000,
  0);

static struct st_mysql_sys_var* cloud_system_variables[]= {
  MYSQL_SYSVAR(enum_var),
  MYSQL_SYSVAR(ulong_var),
  NULL
};

// this is an cloud of SHOW_FUNC and of my_snprintf() service
static int show_func_cloud(MYSQL_THD thd, struct st_mysql_show_var *var,
                             char *buf)
{
  var->type= SHOW_CHAR;
  var->value= buf; // it's of SHOW_VAR_FUNC_BUFF_SIZE bytes
  my_snprintf(buf, SHOW_VAR_FUNC_BUFF_SIZE,
              "enum_var is %lu, ulong_var is %lu, %.6b", // %b is MySQL extension
              srv_enum_var, srv_ulong_var, "really");
  return 0;
}

static struct st_mysql_show_var func_status[]=
{
  {"cloud_func_cloud",  (char *)show_func_cloud, SHOW_FUNC},
  {0,0,SHOW_UNDEF}
};

mysql_declare_plugin(cloud)
{
  MYSQL_STORAGE_ENGINE_PLUGIN,
  &cloud_storage_engine,
  "cloud",
  "Near Infinity Corporation",
  "Hbase storage engine",
  PLUGIN_LICENSE_GPL,
  cloud_init_func,                            /* Plugin Init */
  cloud_done_func,                            /* Plugin Deinit */
  0x0001 /* 0.1 */,
  func_status,                                  /* status variables */
  cloud_system_variables,                     /* system variables */
  NULL,                                         /* config options */
  0,                                            /* flags */
}
mysql_declare_plugin_end;
