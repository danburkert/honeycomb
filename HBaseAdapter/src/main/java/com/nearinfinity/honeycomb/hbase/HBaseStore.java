package com.nearinfinity.honeycomb.hbase;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.BiMap;
import com.nearinfinity.honeycomb.Store;
import com.nearinfinity.honeycomb.Table;
import com.nearinfinity.honeycomb.TableNotFoundException;
import com.nearinfinity.honeycomb.hbaseclient.Constants;
import com.nearinfinity.honeycomb.hbaseclient.SqlTableCreator;
import com.nearinfinity.honeycomb.mysql.Util;
import com.nearinfinity.honeycomb.mysql.gen.TableSchema;
import com.nearinfinity.honeycomb.mysqlengine.HTableFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import static java.text.MessageFormat.format;

public class HBaseStore implements Store {
    private static final String HBASE_TABLE = "nic";
    private static final String CONFIG_PATH = "/etc/mysql/honeycomb.xml";
    private static final HBaseStore ourInstance = new HBaseStore();
    private static volatile boolean isInitialized;
    private final int DEFAULT_TABLE_POOL_SIZE = 5;
    private final long DEFAULT_WRITE_BUFFER_SIZE = 5 * 1024 * 1024;
    private final Logger logger = Logger.getLogger(HBaseStore.class);
    private LoadingCache<String, Long> tableCache;
    private LoadingCache<Long, BiMap<String, Long>> columnsCache;
    private LoadingCache<Long, Long> rowsCache;
    private LoadingCache<Long, Long> autoIncCache;
    private LoadingCache<Long, TableSchema> schemaCache;
    private HTablePool hTablePool = null;

    private HBaseStore() {
    }

    public static HBaseStore getInstance() throws IOException, SAXException, ParserConfigurationException {
        if (!isInitialized) {
            synchronized (ourInstance) {
                if (!isInitialized) {
                    ourInstance.doInitialization();
                }
            }
        }

        return ourInstance;
    }

    @Override
    public Table openTable(String tableName) throws Exception {
        return new HBaseTable(getFreshHTable(), tableCache.get(tableName));
    }

    @Override
    public TableSchema getTableMetadata(String tableName) throws Exception {
        return schemaCache.get(tableCache.get(tableName));
    }

    @Override
    public Table createTable(String tableName, TableSchema schema) throws Exception {
        HTableInterface hTable = getFreshHTable();
        getHBaseMetadata(hTable).putSchema(tableName, schema);
        return new HBaseTable(hTable, tableCache.get(tableName));
    }

    @Override
    public void deleteTable(String tableName) throws Exception {
        HTableInterface hTable = getFreshHTable();
        HBaseMetadata metadata = getHBaseMetadata(hTable);
        metadata.deleteSchema(tableName);
        invalidateCache(tableName);
        hTable.close();
    }

    @Override
    public void alterTable(String tableName, TableSchema schema) throws Exception {
        long tableId = tableCache.get(tableName);
        HTableInterface hTable = getFreshHTable();
        HBaseMetadata metadata = getHBaseMetadata(hTable);
        metadata.updateSchema(tableId, schemaCache.get(tableId), schema);
        invalidateCache(tableName);
        hTable.close();
    }

    @Override
    public void renameTable(String curTableName, String newTableName) throws Exception {
        HTableInterface freshHTable = getFreshHTable();
        HBaseMetadata metadata = getHBaseMetadata(freshHTable);
        metadata.renameExistingTable(curTableName, newTableName);
        invalidateCache(curTableName);
        freshHTable.close();
    }

    @Override
    public long getAutoInc(String tableName) throws Exception {
        return autoIncCache.get(tableCache.get(tableName));
    }

    @Override
    public long incrementAutoInc(String tableName, long amount) throws Exception {
        long tableId = tableCache.get(tableName);
        HTableInterface hTable = getFreshHTable();
        HBaseMetadata metadata = getHBaseMetadata(hTable);
        long value = metadata.incrementAutoInc(tableId, amount);
        autoIncCache.put(tableId, value);
        hTable.close();
        return value;
    }

    @Override
    public void truncateAutoInc(String tableName) throws Exception {
        long tableId = tableCache.get(tableName);
        HTableInterface hTable = getFreshHTable();
        HBaseMetadata metadata = getHBaseMetadata(hTable);
        metadata.truncateAutoInc(tableId);
        autoIncCache.invalidate(tableId);
        hTable.close();
    }

    @Override
    public long getRowCount(String tableName) throws Exception {
        return rowsCache.get(tableCache.get(tableName));
    }

    public long incrementRowCount(long tableId, long amount) throws Exception {
        HTableInterface hTable = getFreshHTable();
        HBaseMetadata metadata = getHBaseMetadata(hTable);
        long value = metadata.incrementRowCount(tableId, amount);
        rowsCache.put(tableId, value);
        hTable.close();
        return value;
    }

    public void truncateRowCount(long tableId) throws Exception {
        HTableInterface hTable = getFreshHTable();
        HBaseMetadata metadata = getHBaseMetadata(hTable);
        metadata.truncateRowCount(tableId);
        rowsCache.invalidate(tableId);
        hTable.close();
    }

    private void doInitialization() throws ParserConfigurationException, IOException, SAXException {
        if (isInitialized) {
            return;
        }

        File configFile = new File(CONFIG_PATH);
        Configuration params;
        try {
            if (!(configFile.exists() && configFile.canRead() && configFile.isFile())) {
                throw new FileNotFoundException(CONFIG_PATH + " doesn't exist or cannot be read.");
            }
            params = Util.readConfiguration(configFile);
            logger.info(format("Read in {0} parameters.", params.size()));
            String hTableName = params.get(Constants.HBASE_TABLE);
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
            throw e;
        } catch (SAXException e) {
            logger.fatal("Exception while trying to parse the config file.", e);
            throw e;
        } catch (ZooKeeperConnectionException e) {
            logger.fatal("Could not connect to zookeeper. ", e);
            throw e;
        } catch (IOException e) {
            logger.fatal("Could not create HBaseStore. Aborting initialization.");
            throw e;
        }

        tableCache = CacheBuilder
                .newBuilder()
                .build(new CacheLoader<String, Long>() {
                    @Override
                    public Long load(String tableName)
                            throws IOException, TableNotFoundException {
                        HTableInterface hTable = getFreshHTable();
                        long id = getHBaseMetadata(hTable).getTableId(tableName);
                        hTable.close();
                        return id;
                    }
                }
                );

        columnsCache = CacheBuilder
                .newBuilder()
                .build(new CacheLoader<Long, BiMap<String, Long>>() {
                    @Override
                    public BiMap<String, Long> load(Long tableId)
                            throws IOException, TableNotFoundException {
                        HTableInterface hTable = getFreshHTable();
                        BiMap<String, Long> columns =
                                getHBaseMetadata(hTable).getColumnIds(tableId);
                        hTable.close();
                        return columns;
                    }
                }
                );

        autoIncCache = CacheBuilder
                .newBuilder()
                .build(new CacheLoader<Long, Long>() {
                    @Override
                    public Long load(Long tableId)
                            throws IOException, TableNotFoundException {
                        HTableInterface hTable = getFreshHTable();
                        Long columns = getHBaseMetadata(hTable).getAutoInc(tableId);
                        hTable.close();
                        return columns;
                    }
                }
                );

        rowsCache = CacheBuilder
                .newBuilder()
                .build(new CacheLoader<Long, Long>() {
                    @Override
                    public Long load(Long tableId)
                            throws IOException, TableNotFoundException {
                        HTableInterface hTable = getFreshHTable();
                        Long columns = getHBaseMetadata(hTable).getRowCount(tableId);
                        hTable.close();
                        return columns;
                    }
                }
                );

        schemaCache = CacheBuilder
                .newBuilder()
                .build(new CacheLoader<Long, TableSchema>() {
                    @Override
                    public TableSchema load(Long tableId)
                            throws IOException, TableNotFoundException {
                        HTableInterface hTable = getFreshHTable();
                        TableSchema schema =
                                getHBaseMetadata(hTable).getSchema(tableId);
                        hTable.close();
                        return schema;
                    }
                }
                );
        isInitialized = true;
    }

    private void invalidateCache(String tableName) throws Exception {
        long tableId = tableCache.get(tableName);
        tableCache.invalidate(tableName);
        columnsCache.invalidate(tableId);
        schemaCache.invalidate(tableId);
    }

    private HBaseMetadata getHBaseMetadata(HTableInterface hTable) {
        return new HBaseMetadata(hTable);
    }

    private HTableInterface getFreshHTable() {
        return hTablePool.getTable(HBASE_TABLE);
    }
}