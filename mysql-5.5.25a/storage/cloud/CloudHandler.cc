#include "sql_priv.h"
#include "sql_class.h"           // MYSQL_HANDLERTON_INTERFACE_VERSION
#include "CloudHandler.h"
#include "probes_mysql.h"
#include "sql_plugin.h"

/*  Undefining min and max macros defined by MySQL, because they cause problems
 *  with the STL min and max functions (thrift includes the STL)
*/
#ifdef min
#undef min
#endif
#ifdef max
#undef max
#endif

#include <sys/socket.h>
#include <arpa/inet.h>

CloudHandler::ha_cloud(handlerton *hton, TABLE_SHARE *table_arg)
  :handler(hton, table_arg)
{}

int CloudHandler::open(const char *name, int mode, uint test_if_locked)
{
  DBUG_ENTER("CloudHandler::open");

  if (!(share = get_share(name, table)))
    DBUG_RETURN(1);
  thr_lock_data_init(&share->lock,&lock,NULL);
  //shared_ptr<TTransport> socket(new TSocket("localhost", 9090));
  //shared_ptr<TTransport> transport(new TBufferedTransport(socket));
  //shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
  //EngineClient client(protocol);

  //try {
  //transport->open();

  //client.open();

  //transport->close();
  //} catch (TException &tx) {
  //printf("ERROR: %s\n", tx.what());
  //} 
  DBUG_RETURN(0);
}

int CloudHandler::close(void)
{
  DBUG_ENTER("CloudHandler::close");
  DBUG_RETURN(free_share(share));
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
