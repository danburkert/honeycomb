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
*/

#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation        // gcc: Class implementation
#endif

#include "sql_priv.h"
#include "sql_class.h"           // MYSQL_HANDLERTON_INTERFACE_VERSION
#include "ha_cloud.h"
#include "probes_mysql.h"
#include "sql_plugin.h"

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
    DBUG_ENTER("ha_cloud::cloud_init_func");

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

    DBUG_PRINT("Java", ("Starting up the jvm"));
    JavaVMInitArgs vm_args;
    JavaVMOption option[1];

    option[0].optionString = "-Djava.class.path=/Users/showell/cloud/HBaseAdapter/target/mysqlengine-0.1-jar-with-dependencies.jar";

    JNI_GetDefaultJavaVMInitArgs(&vm_args);
    vm_args.version = JNI_VERSION_1_6;
    vm_args.options = option;

    JNI_CreateJavaVM(&jvm, (void**)&env, &vm_args);
    DBUG_PRINT("Java", ("Jvm successfully started"));

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
    HBaseAdapter* adapter = new HBaseAdapter(jvm);
    return new (mem_root) CloudHandler(hton, table, &cloud_mutex, &cloud_open_tables, adapter);
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
    "Hbase storage engine",
    PLUGIN_LICENSE_GPL,
    cloud_init_func,                            /* Plugin Init */
    cloud_done_func,                            /* Plugin Deinit */
    0x0001 /* 0.1 */,
    NULL,                                  /* status variables */
    NULL,                     /* system variables */
    NULL,                                         /* config options */
    0,                                            /* flags */
}
mysql_declare_plugin_end;
