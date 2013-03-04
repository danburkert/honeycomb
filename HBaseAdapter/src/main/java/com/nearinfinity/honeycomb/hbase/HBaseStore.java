package com.nearinfinity.honeycomb.hbase;

import com.nearinfinity.honeycomb.Store;
import com.nearinfinity.honeycomb.Table;
import com.nearinfinity.honeycomb.TableNotFoundException;
import com.nearinfinity.honeycomb.hbaseclient.Constants;
import com.nearinfinity.honeycomb.hbaseclient.SqlTableCreator;
import com.nearinfinity.honeycomb.mysql.Util;
import com.nearinfinity.honeycomb.mysql.gen.TableMetadata;
import com.nearinfinity.honeycomb.mysqlengine.HTableFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import static java.text.MessageFormat.format;

public class HBaseStore implements Store {
    private static boolean isInitialized = true;
    private static HBaseStore ourInstance = new HBaseStore();

    private final int DEFAULT_TABLE_POOL_SIZE = 5;
    private final long DEFAULT_WRITE_BUFFER_SIZE = 5 * 1024 * 1024;
    private static final String CONFIG_PATH = "/etc/mysql/honeycomb.xml";

    private final Logger logger = Logger.getLogger(HBaseStore.class);
    private  HTablePool hTablePool;
    private  String hTableName;

    public static HBaseStore getInstance() throws RuntimeException {
        if (isInitialized) {
            return ourInstance;
        } else {
            throw new RuntimeException("HBaseStore has not been initialized correctly.");
        }
    }

    private HBaseStore() {
        File configFile = new File(CONFIG_PATH);
        Configuration params;
        isInitialized = true;
        try {
            if (!(configFile.exists() && configFile.canRead() && configFile.isFile())) {
                throw new FileNotFoundException(CONFIG_PATH + " doesn't exist or cannot be read.");
            }
            params = Util.readConfiguration(configFile);
            logger.info(format("Read in {0} parameters.", params.size()));
            hTableName = params.get(Constants.HBASE_TABLE);
            String zkQuorum = params.get(Constants.ZK_QUORUM);
            long writeBufferSize = params.getLong("write_buffer_size", DEFAULT_WRITE_BUFFER_SIZE);
            int poolSize = params.getInt("honeycomb.pool_size", DEFAULT_TABLE_POOL_SIZE);
            boolean autoFlush = params.getBoolean("flush_changes_immediately", false);

            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", zkQuorum);
            configuration.set(Constants.HBASE_TABLE, hTableName);
            SqlTableCreator.initializeSqlTable(configuration);
            hTablePool = new HTablePool(configuration, poolSize, new HTableFactory(writeBufferSize, autoFlush));

        } catch (ParserConfigurationException e) {
            logger.fatal("The xml parser was not configured properly.", e);
            isInitialized = false;
        } catch (SAXException e) {
            logger.fatal("Exception while trying to parse the config file.", e);
            isInitialized = false;
        } catch (ZooKeeperConnectionException e) {
            logger.fatal("Could not connect to zookeeper. ", e);
            isInitialized = false;
        } catch (IOException e) {
            logger.fatal("Could not create HBaseStore. Aborting initialization.");
            isInitialized = false;
        }
    }

    @Override
    public Table openTable(String name) throws TableNotFoundException {
        return new HBaseTable(hTablePool.getTable(hTableName), name);
    }

    @Override
    public TableMetadata getTableMetadata(String name) throws TableNotFoundException {
        return null;
    }

    @Override
    public Table createTable(TableMetadata metadata) throws IOException {
        return null;
    }

    @Override
    public void deleteTable(String name) throws IOException {
    }
}
