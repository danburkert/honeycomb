#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation        // gcc: Class implementation
#endif

#include "HoneycombHandler.h"
#include "JNICache.h"
#include "JNISetup.h"
#include "JavaFrame.h"
#include <string.h>
#include "Logging.h"
#include "Macros.h"
#include "Java.h"

const char **HoneycombHandler::bas_ext() const
{
  static const char *honeycomb_exts[] = { NullS };
  return honeycomb_exts;
}

HoneycombHandler::HoneycombHandler(handlerton *hton, TABLE_SHARE *table_share,
    mysql_mutex_t* mutex, HASH* open_tables, JavaVM* jvm, JNICache* cache, jobject handler_proxy)
: handler(hton, table_share),
  share(NULL),
  honeycomb_mutex(mutex),
  honeycomb_open_tables(open_tables),
  env(NULL),
  jvm(jvm),
  cache(cache),
  handler_proxy(handler_proxy),
  row(new Row())
{
  this->ref_length = 16;
  this->rows_written = 0;
  this->failed_key_index = 0;
}

HoneycombHandler::~HoneycombHandler()
{
  delete row;
  attach_thread(this->jvm, &(this->env), "HoneycombHandler::~HoneycombHandler");
  env->DeleteGlobalRef(handler_proxy);
  detach_thread(this->jvm);
}

int HoneycombHandler::open(const char *path, int mode, uint test_if_locked)
{
  const char* location = "HoneycombHandler::open";
  DBUG_ENTER(location);
  int rc = 0;

  if (!(share = get_share(path, table)))
  {
    DBUG_RETURN(1);
  }

  thr_lock_data_init(&share->lock, &lock, (void*) this);

  attach_thread(jvm, &env, location);
  {
    JavaFrame frame(env, 2);
    jstring jtable_name =
      string_to_java_string(env, extract_table_name_from_path(path));
    this->env->CallVoidMethod(handler_proxy, cache->handler_proxy().open_table,
        jtable_name);
    rc |= check_exceptions(env, cache, location);
  }
  detach_thread(jvm);

  DBUG_RETURN(rc);
}

int HoneycombHandler::close(void)
{
  const char* location = "HoneycombHandler::close";
  DBUG_ENTER(location);
  int rc = 0;
  attach_thread(jvm, &env, location);
  {
    JavaFrame frame(env, 2);
    this->env->CallVoidMethod(handler_proxy, cache->handler_proxy().close_table);
    env->DeleteGlobalRef(handler_proxy);
    rc |= check_exceptions(env, cache, location);
    handler_proxy = NULL;
  }
  detach_thread(jvm);
  DBUG_RETURN(rc | free_share(share));
}

int HoneycombHandler::external_lock(THD *thd, int lock_type)
{
  const char* location = "HoneycombHandler::external_lock";
  DBUG_ENTER(location);
  int ret = 0;

  if (lock_type == F_WRLCK || lock_type == F_RDLCK)
  {
    attach_thread(jvm, &env, location);
  }

  if (lock_type == F_UNLCK)
  {
    if (this->rows_written > 0)
    {
      this->env->CallVoidMethod(handler_proxy,
          cache->handler_proxy().increment_row_count, this->rows_written);
      check_exceptions(env, cache, location);
      this->rows_written = 0;
    }
    ret |= this->flush();
    detach_thread(jvm);
  }
  DBUG_RETURN(0);
}

THR_LOCK_DATA **HoneycombHandler::store_lock(THD *thd, THR_LOCK_DATA **to,
    enum thr_lock_type lock_type)
{
  if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK)
    lock.type = lock_type;
  *to++ = &lock;
  return to;
}

/*
 Free lock controls.
 */
int HoneycombHandler::free_share(HoneycombShare *share)
{
  DBUG_ENTER("HoneycombHandler::free_share");
  mysql_mutex_lock(honeycomb_mutex);
  int result_code = 0;
  if (!--share->use_count)
  {
    my_hash_delete(honeycomb_open_tables, (uchar*) share);
    thr_lock_delete(&share->lock);
    my_free(share);
  }

  mysql_mutex_unlock(honeycomb_mutex);

  DBUG_RETURN(result_code);
}

ha_rows HoneycombHandler::records_in_range(uint inx, key_range *min_key,
    key_range *max_key)
{
  return stats.records;
}

// MySQL calls this function all over the place whenever it needs you to update
// some crucial piece of info. It expects you to use this to set information
// about your indexes and error codes, as well as general info about your engine.
// The bit flags (defined in my_base.h) passed in will vary depending on what
// it wants you to update during this call. - ABC
int HoneycombHandler::info(uint flag)
{
  // TODO: Update this function to take into account the flag being passed in,
  // like the other engines
  const char* location = "HoneycombHandler::info";
  DBUG_ENTER(location);
  attach_thread(jvm, &env, location);

  ha_rows rec_per_key;

  if (flag & HA_STATUS_VARIABLE)
  {
    JavaFrame frame(env);
    jlong row_count = this->env->CallLongMethod(handler_proxy,
        cache->handler_proxy().get_row_count);
    check_exceptions(env, cache, location);

    if (row_count < 0)
      row_count = 0;
    if (row_count == 0 && !(flag & HA_STATUS_TIME))
      row_count++;

    THD* thd = ha_thd();
    if (thd_sql_command(thd) == SQLCOM_TRUNCATE)
    {
      row_count = 1;
    }

    stats.records = row_count;
    stats.deleted = 0;
    stats.max_data_file_length = this->max_supported_record_length();
    stats.data_file_length = stats.records * this->table->s->reclength;
    stats.index_file_length = this->max_supported_key_length();
    stats.delete_length = stats.deleted * stats.mean_rec_length;
    stats.check_time = 0;

    if (stats.records == 0) {
      stats.mean_rec_length = 0;
    } else {
      stats.mean_rec_length = (ulong) (stats.data_file_length / stats.records);
    }
  }

  if (flag & HA_STATUS_CONST)
  {
    // Update index cardinality - see ::analyze() function for more explanation
    /* Since MySQL seems to favor table scans
       too much over index searches, we pretend
       index selectivity is 2 times better than
       our estimate: */

    for (uint i = 0; i < this->table->s->keys; i++)
    {
      for (uint j = 0; j < table->key_info[i].key_parts; j++)
      {
        rec_per_key = stats.records / 2;

        if (rec_per_key == 0) {
          rec_per_key = 1;
        }

        table->key_info[i].rec_per_key[j] = rec_per_key >= ~(ulong) 0 ?
          ~(ulong) 0 : (ulong) rec_per_key;
      }
    }
  }
  // MySQL needs us to tell it the index of the key which caused the last
  // operation to fail Should be saved in this->failed_key_index for now
  // Later, when we implement transactions, we should use this opportunity to
  // grab the info from the trx itself.
  if (flag & HA_STATUS_ERRKEY)
  {
    this->errkey = this->failed_key_index;
    this->failed_key_index = -1;
  }
  if ((flag & HA_STATUS_AUTO) && table->found_next_number_field) {
    jlong auto_inc_value = env->CallLongMethod(handler_proxy,
        cache->handler_proxy().get_auto_increment);
    check_exceptions(env, cache, location);
    stats.auto_increment_value = (ulonglong) auto_inc_value;
  }

  detach_thread(jvm);
  DBUG_RETURN(0);
}

HoneycombShare *HoneycombHandler::get_share(const char *table_name, TABLE *table)
{
  HoneycombShare *share;
  char *tmp_path_name;
  uint path_length;

  mysql_mutex_lock(honeycomb_mutex);
  path_length = static_cast<uint>(strlen(table_name));

  /*
     If share is not present in the hash, create a new share and
     initialize its members.
     */
  if (!(share = (HoneycombShare*) my_hash_search(honeycomb_open_tables,
          (uchar*) table_name, path_length)))
  {
    if (!my_multi_malloc(MYF(MY_WME | MY_ZEROFILL), &share, sizeof(*share),
          &tmp_path_name, path_length + 1, NullS))
    {
      mysql_mutex_unlock(honeycomb_mutex);
      return NULL;
    }
  }

  share->use_count = 0;
  share->table_path_length = path_length;
  share->path_to_table = tmp_path_name;
  share->crashed = FALSE;
  share->rows_recorded = 0;

  if (my_hash_insert(honeycomb_open_tables, (uchar*) share))
    goto error;
  thr_lock_init(&share->lock);

  share->use_count++;
  mysql_mutex_unlock(honeycomb_mutex);

  return share;

error:
  mysql_mutex_unlock(honeycomb_mutex);
  my_free(share);

  return NULL;
}

int HoneycombHandler::extra(enum ha_extra_function operation)
{
  DBUG_ENTER("HoneycombHandler::extra");
  DBUG_RETURN(0);
}


/**
 * Reserve auto increment values for use by the optimizer.  See the following
 * for more details:
 *    handler.cc#2394
 *    handler.cc#2669
 * note: `nb` seems to stand for `number`
 */
void HoneycombHandler::get_auto_increment(ulonglong offset, ulonglong increment,
                                 ulonglong nb_desired_values,
                                 ulonglong *first_value,
                                 ulonglong *nb_reserved_values)
{
  const char* location = "HoneycombHandler::get_auto_increment";
  DBUG_ENTER(location);

  jlong value = env->CallLongMethod(handler_proxy,
      cache->handler_proxy().increment_auto_increment, nb_desired_values);
  if (check_exceptions(env, cache, location))
  { // exception thrown, return error code
    *first_value = ~(ulonglong) 0;
    DBUG_VOID_RETURN;
  }

  *first_value = (ulonglong) value;
  *nb_reserved_values = nb_desired_values;
  DBUG_VOID_RETURN;
}

void HoneycombHandler::release_auto_increment()
{
  // Stored functions call this last. Hack to get around MySQL not calling
  // start/end bulk insert on insert in a stored function.
  this->flush();
}

/**
 * Stores the UUID of row into the pos field of the handler.  MySQL
 * uses pos during later rnd_pos calls.
 */
void HoneycombHandler::store_uuid_ref(Row* row)
{
  const char* uuid;
  row->get_UUID(&uuid);
  memcpy(this->ref, uuid, this->ref_length);
}

int HoneycombHandler::analyze(THD* thd, HA_CHECK_OPT* check_opt)
{
  DBUG_ENTER("HoneycombHandler::analyze");

  // For each key, just tell MySQL that there is only one value per keypart.
  // This is, in effect, like telling MySQL that all our indexes are unique,
  // and should essentially always be used for lookups.  If you don't do this,
  // the optimizer REALLY tries to do scans, even when they're not ideal. - ABC

  for (uint i = 0; i < this->table->s->keys; i++)
  {
    for (uint j = 0; j < table->key_info[i].key_parts; j++)
    {
      this->table->key_info[i].rec_per_key[j] = 1;
    }
  }

  DBUG_RETURN(0);
}

/**
 * Estimate the number of rows contained in the table associated with this
 * handler.  Called by the optimizer.
 */
ha_rows HoneycombHandler::estimate_rows_upper_bound()
{
  const char* location = "HoneycombHandler::estimate_rows_upper_bound";
  DBUG_ENTER(location);
  jlong row_count = this->env->CallLongMethod(handler_proxy,
      cache->handler_proxy().get_row_count);
  check_exceptions(env, cache, location);

  // Stupid MySQL and its filesort. This must be large enough to filesort when
  // there are less than 2 records.
  DBUG_RETURN(row_count < 2 ? 10 : 2*row_count + 1);
}

/**
 * Flush writes and deletes.  Must be called from an attached thread.
 */
int HoneycombHandler::flush()
{
  this->env->CallVoidMethod(handler_proxy, cache->handler_proxy().flush);
  return check_exceptions(env, cache, "HoneycombHandler::flush");
}
