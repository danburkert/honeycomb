#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation        // gcc: Class implementation
#endif

#include "sql_priv.h"
#include "sql_class.h"           // MYSQL_HANDLERTON_INTERFACE_VERSION
#include "CloudHandler.h"
#include "probes_mysql.h"
#include "sql_plugin.h"
#include "ha_cloud.h"

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

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>

#include "gen-cpp/Engine.h"

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace boost;

// using namespace com::nearinfinity::hbase_engine;

static HASH cloud_open_tables;

CloudHandler::CloudHandler(handlerton *hton, TABLE_SHARE *table_arg)
  :handler(hton, table_arg)
{}

/*
  If frm_error() is called in table.cc this is called to find out what file
  extensions exist for this handler.

  // TODO: Do any extensions exist for this handler? Doesn't seem like it. - ABC
*/
static const char *cloud_exts[] = {
  NullS
};

const char **bas_ext() const
{
	return cloud_exts;
}

static CloudShare *get_share(const char *table_name, TABLE *table)
{
	CloudShare *share;
	char meta_file_name[FN_REFLEN];
	MY_STAT file_stat;                /* Stat information for the data file */
	char *tmp_name;
	uint length;

	mysql_mutex_lock(&cloud_mutex);
	length=(uint) strlen(table_name);

	/*
	If share is not present in the hash, create a new share and
	initialize its members.
	*/
	if (!(share=(CloudShare*) my_hash_search(&cloud_open_tables,
										   (uchar*) table_name,
										   length)))
	{
	if (!my_multi_malloc(MYF(MY_WME | MY_ZEROFILL),
						 &share, sizeof(*share),
						 &tmp_name, length+1,
						 NullS))
	{
	  mysql_mutex_unlock(&cloud_mutex);
	  return NULL;
	}

	share->use_count= 0;
	share->table_name_length= length;
	share->table_name= tmp_name;
	share->crashed= FALSE;
	share->rows_recorded= 0;
	share->data_file_version= 0;
	strmov(share->table_name, table_name);
	fn_format(share->data_file_name, table_name, "", NullS,
			  MY_REPLACE_EXT|MY_UNPACK_FILENAME);

	if (my_hash_insert(&cloud_open_tables, (uchar*) share))
	  goto error;
	thr_lock_init(&share->lock);

	share->use_count++;
	mysql_mutex_unlock(&cloud_mutex);

	return share;

	error:
	mysql_mutex_unlock(&tina_mutex);
	my_free(share);

	return NULL;
}

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
  DBUG_ENTER("CloudHandler::rnd_pos");my_off_t saved_data_file_length;
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
static int free_share(CloudShare *share)
{
  DBUG_ENTER("CloudHandler::free_share");
  mysql_mutex_lock(&cloud_mutex);
  int result_code= 0;
  if (!--share->use_count){
    my_hash_delete(&cloud_open_tables, (uchar*) share);
    thr_lock_delete(&share->lock);
    mysql_mutex_destroy(&share->mutex);
    my_free(share);
  }
  mysql_mutex_unlock(&cloud_mutex);

  DBUG_RETURN(result_code);
}
