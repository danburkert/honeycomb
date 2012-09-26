#include "Transaction.h"

char *get_log_file_name(THD *thd)
{
  char file_path[FN_REFLEN];

  sprintf(file_path, "cloud-trx-log-%lu-%lu", thd->id, time(NULL));

  return file_path;
}

// Creates and initializes a transaction object.
cloud_trx* new_cloud_trx(THD *thd, handlerton *cloud_hton)
{
  cloud_trx *trx = (cloud_trx *) my_malloc(sizeof(cloud_trx), MYF(MY_WME));
  thd->ha_data[cloud_hton->slot].ha_ptr = trx;

  trx->mysql_thd = thd;
  trx->log_file_name = get_log_file_name(thd);
  trx->log_file_offset = 0;
  trx->lock_count = 0;

  return trx;
}

void delete_cloud_trx(THD *thd, handlerton *cloud_hton)
{
  cloud_trx *&trx = thd_get_trx(thd, cloud_hton);

  if (trx != NULL)
  {
    trx->log_file_name = NULL;
    trx->log_file_offset = NULL;
    trx->mysql_thd = NULL;

    my_free(trx);
    trx = NULL;
  }
}

cloud_trx *get_current_transaction(THD *thd, handlerton *cloud_hton)
{
  cloud_trx *&trx = thd_get_trx(thd, cloud_hton);

  return trx;
}

// Quick way to retrieve the active transaction object for a given thread
cloud_trx*& thd_get_trx(THD* thd, handlerton *cloud_hton)
{
  return(*(cloud_trx**) thd_ha_data(thd, cloud_hton));
}

// Registers the current SQL statement with the MySQL transaction coordinator
// Must be called for every transaction which may be committed or rolled back
void register_cloud_trx(handlerton* cloud_hton, THD* thd, cloud_trx* trx)
{
  trans_register_ha(thd, FALSE, cloud_hton);

  if (/*!trx_is_registered_for_2pc(trx) && */thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)) {

    trans_register_ha(thd, TRUE, cloud_hton);
  }

  // trx_register_for_2pc(trx);
}
