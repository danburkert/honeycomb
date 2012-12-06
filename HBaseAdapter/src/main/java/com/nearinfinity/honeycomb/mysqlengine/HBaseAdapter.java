package com.nearinfinity.honeycomb.mysqlengine;

import com.google.common.collect.Iterables;
import com.nearinfinity.honeycomb.hbaseclient.*;
import com.nearinfinity.honeycomb.hbaseclient.strategy.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static java.text.MessageFormat.format;

public class HBaseAdapter {
    private static AtomicLong activeScanCounter;
    private static Map<Long, ActiveScan> activeScanLookup;
    private static HBaseClient client;
    private static final Logger logger = Logger.getLogger(HBaseAdapter.class);

    private static final int DEFAULT_NUM_CACHED_ROWS = 2500;
    private static final long DEFAULT_WRITE_BUFFER_SIZE = 5 * 1024 * 1024;
    private static boolean isInitialized = false;

    private static final String CONFIG_PATH = "/etc/mysql/adapter.conf";

    private static final Object initializationLock = new Object();

    public static void initialize() throws IOException {
        // Working with static variables requires locking to ensure consistency.
        synchronized (initializationLock) {
            doInitialization();
        }
    }

    private static void doInitialization() throws IOException {
        if (isInitialized) {
            return;
        }

        logger.info("Begin");

        File configFile = new File(CONFIG_PATH);
        if (!(configFile.exists() && configFile.canRead() && configFile.isFile())) {
            throw new FileNotFoundException(CONFIG_PATH + " doesn't exist or cannot be read.");
        }

        Configuration params = Util.readConfiguration(configFile);
        logger.info(format("Read in {0} parameters.", params.size()));

        try {
            client = new HBaseClient(params.get("hbase_table_name"),
                    params.get("zk_quorum"));
        } catch (ZooKeeperConnectionException e) {
            logger.fatal("Could not connect to zookeeper. ", e);
            throw e;
        } catch (IOException e) {
            logger.fatal("Could not create HBase client. Aborting initialization.");
            throw e;
        }
        logger.info("HBaseClient successfully created.");
        activeScanCounter = new AtomicLong(0L);
        activeScanLookup = new ConcurrentHashMap<Long, ActiveScan>();

        client.setCacheSize(params.getInt("table_scan_cache_rows", DEFAULT_NUM_CACHED_ROWS));
        client.setWriteBufferSize(params.getLong("write_buffer_size", DEFAULT_WRITE_BUFFER_SIZE));
        client.setAutoFlushTables(params.getBoolean("flush_changes_immediately", false));
        isInitialized = true;
        logger.info("End");
    }

    public static boolean createTable(String tableName,
                                      Map<String, ColumnMetadata> columns, TableMultipartKeys multipartKeys)
            throws HBaseAdapterException {
        try {
            logger.info("tableName:" + tableName);
            if (client == null) {
                logger.info("client is null!");
            }

            client.createTableFull(tableName, columns, multipartKeys);

            return true;
        } catch (Throwable e) {
            logger.error("Exception:", e);
            throw new HBaseAdapterException("createTable", e);
        }
    }

    /**
     * Method called when a "show create table tableName" statement is executed.
     *
     * @param tableName
     * @param fieldName
     * @return
     * @throws HBaseAdapterException
     */
    public static long getAutoincrementValue(String tableName, String fieldName) throws HBaseAdapterException {
        long returnValue = -1;

        try {
            logger.info(format("tableName: {0}, fieldName: {1}", tableName, fieldName));
            if (client == null)
                logger.info("client is null!");

            returnValue = client.getAutoincrementValue(tableName, fieldName);
        } catch (Throwable e) {
            logger.error("Exception:", e);
            throw new HBaseAdapterException("alterTableAutoincrementValue", e);
        }

        return returnValue;
    }

    /**
     * Method called when a "alter table tableName auto_increment=autoIncrementValue" statement is executed.
     *
     * @param tableName
     * @param fieldName
     * @param autoincrementValue
     * @param isTruncate
     * @return
     * @throws HBaseAdapterException
     */
    public static boolean alterAutoincrementValue(String tableName, String fieldName, long autoincrementValue, boolean isTruncate) throws HBaseAdapterException {
        boolean returnValue = false;

        try {
            logger.info(format("tableName: {0}, fieldName: {1}, autoIncrementValue: {2}, isTruncate: {3}", tableName, fieldName, autoincrementValue, isTruncate));
            if (client == null)
                logger.info("client is null!");

            returnValue = client.alterAutoincrementValue(tableName, fieldName, autoincrementValue, isTruncate);
        } catch (Throwable e) {
            logger.error("Exception:", e);
            throw new HBaseAdapterException("alterTableAutoincrementValue", e);
        }

        return returnValue;
    }

    public static long startScan(String tableName, boolean isFullTableScan)
            throws HBaseAdapterException {
        try {
            long scanId = activeScanCounter.incrementAndGet();
            logger.info(String.format("tableName: %s, scanId: %s, isFullTableScan: %s", tableName, scanId, isFullTableScan));
            ScanStrategy strategy = new FullTableScanStrategy(tableName);
            SingleResultScanner dataScanner = new SingleResultScanner(client.getScanner(strategy));
            activeScanLookup.put(scanId, new ActiveScan(tableName, dataScanner));
            return scanId;
        } catch (Throwable e) {
            logger.error("Exception:", e);
            throw new HBaseAdapterException("startScan", e);
        }
    }

    public static Row nextRow(long scanId) throws HBaseAdapterException {
        try {
            Row row = new Row();
            ActiveScan conn = getActiveScanForId(scanId);
            HBaseResultScanner scanner = conn.getScanner();
            Result result = scanner.next(null);

            if (result == null) {
                return null;
            }

            TableInfo info = client.getTableInfo(conn.getTableName());
            Map<String, byte[]> values = ResultParser.parseDataRow(result, info);
            UUID uuid = ResultParser.parseUUID(result);
            row.parse(values, uuid);
            return row;
        } catch (Throwable e) {
            logger.error("Exception:", e);
            throw new HBaseAdapterException("nextRow", e);
        }
    }

    public static void endScan(long scanId) throws HBaseAdapterException {
        logger.info("scanId: " + scanId);
        try {
            if (!activeScanLookup.containsKey(scanId)) {
                return;
            }

            ActiveScan conn = getActiveScanForId(scanId);
            conn.close();
            activeScanLookup.remove(scanId);
        } catch (Throwable e) {
            logger.error("Exception:", e);
            throw new HBaseAdapterException("endScan", e);
        }
    }

    public static boolean writeRow(String tableName, Map<String, byte[]> values)
            throws HBaseAdapterException {
        try {
            client.writeRow(tableName, values);
        } catch (Exception e) {
            logger.error("Exception:", e);
            throw new HBaseAdapterException("writeRow", e);
        }

        return true;
    }

    public static void flushWrites() {
        client.flushWrites();
    }

    public static boolean deleteRow(long scanId) throws HBaseAdapterException {
        logger.info("scanId: " + scanId);
        try {
            ActiveScan activeScan = getActiveScanForId(scanId);
            HBaseResultScanner scanner = activeScan.getScanner();
            Result result = scanner.getLastResult();
            String tableName = activeScan.getTableName();
            UUID uuid = ResultParser.parseUUID(result);

            return client.deleteRow(tableName, uuid);
        } catch (Throwable e) {
            logger.error("Exception:", e);
            throw new HBaseAdapterException("deleteRow", e);
        }
    }

    public static int deleteAllRows(String tableName) throws HBaseAdapterException {
        logger.info("tableName: " + tableName);

        try {
            return client.deleteAllRows(tableName);
        } catch (Throwable e) {
            logger.error("Exception:", e);
            throw new HBaseAdapterException("deleteAllRows", e);
        }
    }

    public static boolean dropTable(String tableName) throws HBaseAdapterException {
        logger.info("tableName: " + tableName);
        try {
            return client.dropTable(tableName);
        } catch (Throwable e) {
            logger.error("Exception:", e);
            throw new HBaseAdapterException("dropTable", e);
        }
    }

    public static Row getRow(long scanId, byte[] uuid) throws HBaseAdapterException {
        logger.info(String.format("scanId: %d,%s", scanId, Bytes.toString(uuid)));
        try {
            Row row = new Row();
            ActiveScan activeScan = getActiveScanForId(scanId);
            String tableName = activeScan.getTableName();
            ByteBuffer buffer = ByteBuffer.wrap(uuid);
            UUID rowUuid = new UUID(buffer.getLong(), buffer.getLong());

            Result result = client.getDataRow(rowUuid, tableName);

            if (result == null) {
                logger.error("Exception: Row not found");
                throw new HBaseAdapterException("getRow", new Exception());
            }

            activeScan.getScanner().setLastResult(result);

            TableInfo info = client.getTableInfo(activeScan.getTableName());
            Map<String, byte[]> values = ResultParser.parseDataRow(result, info);
            row.setUUID(rowUuid);
            row.setRowMap(values);
            return row;
        } catch (Throwable e) {
            logger.error("Exception:", e);
            throw new HBaseAdapterException("getRow", e);
        }

    }

    public static long startIndexScan(String tableName, String columnName)
            throws HBaseAdapterException {
        logger.info(String.format("tableName %s, columnNames: %s", tableName, columnName));
        try {
            long scanId = activeScanCounter.incrementAndGet();
            activeScanLookup.put(scanId, new ActiveScan(tableName, columnName));
            return scanId;
        } catch (Throwable e) {
            logger.error("Exception:", e);
            throw new HBaseAdapterException("startIndexScan", e);
        }
    }

    public static String findDuplicateKey(String tableName, Map<String, byte[]> values)
            throws HBaseAdapterException {
        try {
            return client.findDuplicateKey(tableName, values);
        } catch (Throwable e) {
            logger.error("Exception:", e);
            throw new HBaseAdapterException("hasDuplicateValues", e);
        }
    }

    public static byte[] findDuplicateValue(String tableName, String columnName)
            throws HBaseAdapterException {
        try {
            return client.findDuplicateValue(tableName, columnName);
        } catch (Throwable e) {
            logger.error("Exception:", e);
            throw new HBaseAdapterException("columnContainsDuplicates", e);
        }
    }

    public static long getNextAutoincrementValue(String tableName, String columnName)
            throws HBaseAdapterException {
        try {
            return client.getNextAutoincrementValue(tableName, columnName);
        } catch (Exception e) {
            logger.error("Exception:", e);
            throw new HBaseAdapterException("columnContainsDuplicates", e);
        }
    }

    public static IndexRow indexRead(long scanId, List<KeyValue> keyValues,
                                     IndexReadType readType)
            throws HBaseAdapterException {
        try {
            IndexRow indexRow = new IndexRow();
            ActiveScan activeScan = getActiveScanForId(scanId);
            String tableName = activeScan.getTableName();
            List<String> columnName = activeScan.getColumnName();
            if (keyValues == null) {
                if (readType != IndexReadType.INDEX_FIRST
                        && readType != IndexReadType.INDEX_LAST) {
                    throw new IllegalArgumentException("keyValues can't be null unless first/last index read");
                }

                keyValues = new LinkedList<KeyValue>();
                byte fill = (byte) (readType == IndexReadType.INDEX_FIRST ? 0x00 : 0xFF);
                client.setupKeyValues(tableName, columnName, keyValues, fill);
            }

            ScanStrategyInfo scanInfo = new ScanStrategyInfo(tableName, columnName, keyValues);

            byte[] valueToSkip = null;
            HBaseResultScanner scanner = null;

            switch (readType) {
                case HA_READ_KEY_EXACT: {
                    ScanStrategy strategy = new PrefixScanStrategy(scanInfo);
                    scanner = new SingleResultScanner(client.getScanner(strategy));
                }
                break;
                case HA_READ_AFTER_KEY: {
                    ScanStrategy strategy = new OrderedScanStrategy(scanInfo);
                    scanner = new SingleResultScanner(client.getScanner(strategy));
                    valueToSkip = Iterables.getLast(scanInfo.keyValueValues());
                }
                break;
                case HA_READ_KEY_OR_NEXT: {
                    ScanStrategy strategy = new OrderedScanStrategy(scanInfo);
                    scanner = new SingleResultScanner(client.getScanner(strategy));
                }
                break;
                case HA_READ_BEFORE_KEY: {
                    ScanStrategy strategy = new ReverseScanStrategy(scanInfo);
                    scanner = new SingleResultScanner(client.getScanner(strategy));
                    valueToSkip = Iterables.getLast(scanInfo.keyValueValues());
                }
                break;
                case HA_READ_KEY_OR_PREV: {
                    ScanStrategy strategy = new ReverseScanStrategy(scanInfo);
                    scanner = new SingleResultScanner(client.getScanner(strategy));
                }
                break;
                case INDEX_FIRST: {
                    ScanStrategy strategy = new OrderedScanStrategy(scanInfo, true);
                    scanner = new SingleResultScanner(client.getScanner(strategy));
                }
                break;
                case INDEX_LAST: {
                    ScanStrategy strategy = new ReverseScanStrategy(scanInfo, true);
                    scanner = new SingleResultScanner(client.getScanner(strategy));
                }
                break;
            }

            scanner.setColumnName(Iterables.getLast(scanInfo.keyValueColumns()));

            activeScan.setScanner(scanner);
            Result result = scanner.next(valueToSkip);

            if (result == null) {
                return indexRow;
            }

            indexRow.parseResult(result);
            return indexRow;
        } catch (Throwable e) {
            logger.error("Exception:", e);
            throw new HBaseAdapterException("indexRead", e);
        }
    }

    public static IndexRow nextIndexRow(long scanId) throws HBaseAdapterException {
        try {
            IndexRow indexRow = new IndexRow();
            ActiveScan conn = getActiveScanForId(scanId);
            HBaseResultScanner scanner = conn.getScanner();
            Result result = scanner.next(null);
            if (result == null) {
                return indexRow;
            }

            indexRow.parseResult(result);
            return indexRow;
        } catch (Throwable e) {
            logger.error("Exception:", e);
            throw new HBaseAdapterException("nextIndexRow", e);
        }
    }

    private static ActiveScan getActiveScanForId(long scanId) throws HBaseAdapterException {
        ActiveScan conn = activeScanLookup.get(scanId);
        if (conn == null) {
            throw new HBaseAdapterException("No connection for scanId: " + scanId, null);
        }
        return conn;
    }

    public static void incrementRowCount(String tableName, long delta) throws HBaseAdapterException {
        try {
            client.incrementRowCount(tableName, delta);
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw new HBaseAdapterException("incrementRowCount", e);
        }
    }

    public static void setRowCount(String tableName, long delta) throws HBaseAdapterException {
        try {
            client.setRowCount(tableName, delta);
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw new HBaseAdapterException("setRowCount", e);
        }
    }

    public static long getRowCount(String tableName) throws HBaseAdapterException {
        try {
            return client.getRowCount(tableName);
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw new HBaseAdapterException("getRowCount", e);
        }
    }

    public static void renameTable(String from, String to) throws HBaseAdapterException {
        try {
            client.renameTable(from, to);
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw new HBaseAdapterException("renameTable", e);
        }
    }

    public static boolean isNullable(String tableName, String columnName) throws HBaseAdapterException {
        try {
            return client.isNullable(tableName, columnName);
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw new HBaseAdapterException("isNullable", e);
        }
    }

    public static void addIndex(String tableName, TableMultipartKeys columnsToIndex) throws HBaseAdapterException {
        try {
            client.addIndex(tableName, columnsToIndex);
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw new HBaseAdapterException("addIndex", e);
        }
    }

    public static void dropIndex(String tableName, String indexToDrop) throws HBaseAdapterException {
        try {
            client.dropIndex(tableName, indexToDrop);
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw new HBaseAdapterException("dropIndex", e);
        }
    }
}
