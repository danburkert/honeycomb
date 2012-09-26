#ifndef TRANSACTION_H
#define TRANSACTION_H

#define MYSQL_SERVER 1

#include "sql_class.h"

// Keep all transaction-related data you need here in this struct
struct cloud_trx {
  const char* log_file_name;
  THD *mysql_thd;
  ulonglong log_file_offset;
  ulong lock_count; // measures the number of mysql tables in use...when it hits zero, we should trigger a commit
  ulong tx_isolation;
};

char *get_log_file_name(THD *thd);
cloud_trx* new_cloud_trx(THD *thd, handlerton *cloud_hton);
cloud_trx* get_current_transaction(THD *thd, handlerton *cloud_hton);
cloud_trx*& thd_get_trx(THD* thd, handlerton *cloud_hton);
void register_cloud_trx(handlerton* cloud_hton, THD* thd, cloud_trx* trx);
void delete_cloud_trx(THD *thd, handlerton *cloud_hton);

// Commits a transaction or marks an SQL statement ended.
static int cloud_commit(handlerton *hton, THD* thd, bool all)
{
  DBUG_ENTER("cloud_commit");
  DBUG_PRINT("trans", ("ending transaction"));

  if (all || (!thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)))
  {
    /* We were instructed to commit the whole transaction, or
    this is an SQL statement end and autocommit is on */

    delete_cloud_trx(thd, hton);
  }
  else
  {
    /* We just mark the SQL statement ended and do not do a
    transaction commit */

    /* Store the current undo_no of the transaction so that we
    know where to roll back if we have to roll back the next
    SQL statement */
  }

  DBUG_RETURN(0);
}

// Rolls back a transaction or the latest SQL statement.
static int cloud_rollback(handlerton *hton, THD* thd, bool all)
{
  // If all is true, roll back everything
  // If all is false, only roll back the current statement

  DBUG_ENTER("cloud_rollback");
  DBUG_PRINT("trans", ("aborting transaction"));

  if (all || !thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN))
  {

  }
  else
  {

  }

  // This should be some kind of error code that you return, most likely
  DBUG_RETURN(0);
}

#endif
