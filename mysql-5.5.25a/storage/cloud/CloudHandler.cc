#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation        // gcc: Class implementation
#endif

#include "sql_priv.h"
#include "sql_class.h"           // MYSQL_HANDLERTON_INTERFACE_VERSION
#include "CloudHandler.h"
#include "probes_mysql.h"
#include "sql_plugin.h"
#include "ha_cloud.h"
#include <jni.h>
/*
  If frm_error() is called in table.cc this is called to find out what file
  extensions exist for this handler.

  // TODO: Do any extensions exist for this handler? Doesn't seem like it. - ABC
*/
const char **CloudHandler::bas_ext() const
{
    static const char *cloud_exts[] =
    {
        NullS
    };

    return cloud_exts;
}

int CloudHandler::open(const char *name, int mode, uint test_if_locked)
{
    DBUG_ENTER("CloudHandler::open");

    if (!(share = get_share(name, table)))
    {
        DBUG_RETURN(1);
    }

    thr_lock_data_init(&share->lock, &lock, (void*) this);
    DBUG_PRINT("Java", ("Starting up the jvm"));
    JavaVM* jvm;
    JNIEnv* env;
    JavaVMInitArgs vm_args;
    JavaVMOption option[1];

    option[0].optionString = "-Djava.class.path=/Users/{home}/Development/jni-test";

    JNI_GetDefaultJavaVMInitArgs(&vm_args);
    vm_args.version = JNI_VERSION_1_6;
    vm_args.options = option;

    JNI_CreateJavaVM(&jvm, (void**)&env, &vm_args);
    jclass cls = env->FindClass("HelloWorld");
    jmethodID mid = env->GetStaticMethodID(cls, "test", "(I)V");
    env->CallStaticVoidMethod(cls, mid, 100);
    jvm->DestroyJavaVM();
    DBUG_RETURN(0);
}

int CloudHandler::close(void)
{
    DBUG_ENTER("CloudHandler::close");
    DBUG_RETURN(free_share(share));
}

int CloudHandler::write_row(uchar *buf)
{
    DBUG_ENTER("CloudHandler::write_row");
    DBUG_RETURN(0);
}

int CloudHandler::update_row(const uchar *old_data, uchar *new_data)
{
    DBUG_ENTER("CloudHandler::update_row");
    DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

int CloudHandler::delete_row(const uchar *buf)
{
    DBUG_ENTER("CloudHandler::delete_row");
    DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

int CloudHandler::rnd_init(bool scan)
{
    DBUG_ENTER("CloudHandler::rnd_init");
    DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}

int CloudHandler::external_lock(THD *thd, int lock_type)
{
    DBUG_ENTER("CloudHandler::external_lock");
    DBUG_RETURN(0);
}

int CloudHandler::rnd_next(uchar *buf)
{
    int rc;
    DBUG_ENTER("CloudHandler::rnd_next");
    MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str, TRUE);
    rc= HA_ERR_END_OF_FILE;
    MYSQL_READ_ROW_DONE(rc);
    DBUG_RETURN(rc);
}

void CloudHandler::position(const uchar *record)
{
    DBUG_ENTER("CloudHandler::position");
    DBUG_VOID_RETURN;
}

int CloudHandler::rnd_pos(uchar *buf, uchar *pos)
{
    int rc;
    DBUG_ENTER("CloudHandler::rnd_pos");
    my_off_t saved_data_file_length;
    MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str,
                         TRUE);
    rc= HA_ERR_WRONG_COMMAND;
    MYSQL_READ_ROW_DONE(rc);
    DBUG_RETURN(rc);
}

int CloudHandler::create(const char *name, TABLE *table_arg,
                         HA_CREATE_INFO *create_info)
{
    DBUG_ENTER("CloudHandler::create");
    DBUG_RETURN(0);
}

THR_LOCK_DATA **CloudHandler::store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type)
{
    if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK)
        lock.type=lock_type;
    *to++= &lock;
    return to;
}

/*
  Free lock controls.
*/
int CloudHandler::free_share(CloudShare *share)
{
    DBUG_ENTER("CloudHandler::free_share");
    mysql_mutex_lock(cloud_mutex);
    int result_code= 0;
    if (!--share->use_count)
    {
        my_hash_delete(cloud_open_tables, (uchar*) share);
        thr_lock_delete(&share->lock);
        mysql_mutex_destroy(&share->mutex);
        my_free(share);
    }
    mysql_mutex_unlock(cloud_mutex);

    DBUG_RETURN(result_code);
}

int CloudHandler::info(uint)
{
    DBUG_ENTER("CloudHandler::info");
    DBUG_RETURN(0);
}
