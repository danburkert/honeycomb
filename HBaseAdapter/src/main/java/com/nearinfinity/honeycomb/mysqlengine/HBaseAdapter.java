package com.nearinfinity.honeycomb.mysqlengine;

import com.google.common.collect.Iterables;
import com.nearinfinity.honeycomb.hbase.ResultReader;
import com.nearinfinity.honeycomb.hbaseclient.*;
import com.nearinfinity.honeycomb.hbaseclient.strategy.*;
import com.nearinfinity.honeycomb.mysql.Row;
import com.nearinfinity.honeycomb.mysql.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static java.text.MessageFormat.format;

public class HBaseAdapter {
    private static final AtomicLong
            activeScanCounter = new AtomicLong(0L),
            activeWriterCounter = new AtomicLong(0L),
            rowCountCache = new AtomicLong(0L);
    private static final AtomicBoolean rowCountUpdate = new AtomicBoolean(true);
    private static final Map<Long, ActiveScan> activeScanLookup = new ConcurrentHashMap<Long, ActiveScan>();
    private static final Map<Long, HBaseWriter> activeWriterLookup = new ConcurrentHashMap<Long, HBaseWriter>();
    private static final Logger logger = Logger.getLogger(HBaseAdapter.class);
    private static final int DEFAULT_NUM_CACHED_ROWS = 2500;
    private static final long DEFAULT_WRITE_BUFFER_SIZE = 5 * 1024 * 1024;
    private static final int DEFAULT_TABLE_POOL_SIZE = 5;
    private static final String CONFIG_PATH = "/etc/mysql/honeycomb.xml";
    private static final Object initializationLock = new Object();
    private static HBaseReader reader;
    private static HTablePool tablePool;
    private static boolean isInitialized = false;
    private static Configuration params;
    private static long writeBufferSize;

    /**
     * Initializes the resources for connecting with HBase
     *
     * @throws IOException Connecting to HBase failed
     */
    public static void initialize() throws IOException {
        // Working with static variables requires locking to ensure consistency.
        synchronized (initializationLock) {
            doInitialization();
        }
    }

    /**
     * Creates a sql table in HBase with columns and indexes.
     * Called when a "create table XXX()" statement is executed.
     *
     * @param tableName      Name of sql table
     * @param columns        Column name and metadata
     * @param indexedColumns Indexed columns
     * @return Success
     * @throws HBaseAdapterException
     */
    public static boolean createTable(String tableName,
                                      Map<String, ColumnMetadata> columns, TableMultipartKeys indexedColumns)
            throws HBaseAdapterException {
        try {
            long start = System.currentTimeMillis();
            logger.debug("Creating table " + tableName);
            HBaseWriter writer = createWriter();
            writer.createTableFull(tableName, columns, indexedColumns);
            writer.close();
            long end = System.currentTimeMillis();
            Metrics.getInstance().addStat("Create table", end - start);
            return true;
        } catch (Throwable e) {
            logger.error("Exception:", e);
            throw new HBaseAdapterException("createTable", e);
        }
    }

    /**
     * Method called when a "show create table tableName" statement is executed.
     */
    public static long getAutoincrementValue(String tableName, String fieldName) throws HBaseAdapterException {
        long returnValue = -1;

        try {
            if (logger.isDebugEnabled()) {
                logger.debug(format("tableName: {0}, fieldName: {1}", tableName, fieldName));
            }
            returnValue = reader.getAutoincrementValue(tableName, fieldName);
        } catch (Throwable e) {
            logger.error("Exception:", e);
            throw new HBaseAdapterException("alterTableAutoincrementValue", e);
        }

        return returnValue;
    }

    /**
     * Method called when a "alter table tableName auto_increment=autoIncrementValue" statement is executed.
     */
    public static boolean alterAutoincrementValue(String tableName, String fieldName, long autoincrementValue, boolean isTruncate) throws HBaseAdapterException {
        boolean returnValue = false;

        try {
            if (logger.isDebugEnabled()) {
                logger.debug(format("tableName: {0}, fieldName: {1}, autoIncrementValue: {2}, isTruncate: {3}", tableName, fieldName, autoincrementValue, isTruncate));
            }
            HBaseWriter writer = createWriter();
            returnValue = writer.alterAutoincrementValue(tableName, fieldName, autoincrementValue, isTruncate);
            writer.close();
        } catch (Throwable e) {
            logger.error("Exception:", e);
            throw new HBaseAdapterException("alterTableAutoincrementValue", e);
        }

        return returnValue;
    }

    /**
     * Create a "session" for writing data into HBase.
     * To be called before writing any rows.
     *
     * @return A "session" ID for writing
     * @throws HBaseAdapterException
     */
    public static long startWrite() throws HBaseAdapterException {
        try {
            long writerId = activeWriterCounter.incrementAndGet();
            if (logger.isDebugEnabled()) {
                logger.debug(format("New writer {0}", writerId));
            }
            HBaseWriter writer = createWriter();
            activeWriterLookup.put(writerId, writer);
            return writerId;
        } catch (Throwable e) {
            logger.error("Exception", e);
            throw new HBaseAdapterException("startWrite", e);
        }
    }

    /**
     * Ends a "session" for writing data and cleans up resources.
     *
     * @param writeId ID of the "session"
     * @throws HBaseAdapterException
     */
    public static void endWrite(long writeId) throws HBaseAdapterException {
        try {
            if (logger.isDebugEnabled()) {
                logger.debug(format("End writer {0}", writeId));
            }
            HBaseWriter writer = activeWriterLookup.get(writeId);
            if (writer != null) {
                writer.close();
                activeWriterLookup.remove(writeId);
            }

        } catch (Throwable e) {
            logger.error("Exception", e);
            throw new HBaseAdapterException("endWrite", e);
        }
    }

    /**
     * Create a "session" for a full table scan.
     *
     * @param tableName       SQL table name to scan
     * @param isFullTableScan To remove
     * @return scan ID of the "session"
     * @throws HBaseAdapterException
     */
    public static long startScan(String tableName, boolean isFullTableScan)
            throws HBaseAdapterException {
        try {
            long start = System.currentTimeMillis();
            long scanId = activeScanCounter.incrementAndGet();
            ScanStrategy strategy = new FullTableScanStrategy(tableName);
            SingleResultScanner dataScanner = new SingleResultScanner(reader.getScanner(strategy));
            activeScanLookup.put(scanId, new ActiveScan(tableName, dataScanner));
            long end = System.currentTimeMillis();
            Metrics.getInstance().addStat("Start Scan", end - start);
            return scanId;
        } catch (Throwable e) {
            logger.error("Exception:", e);
            throw new HBaseAdapterException("startScan", e);
        }
    }

    /**
     * Retrieve a SQL row from HBase and move cursor forward.
     *
     * @param scanId The "session" ID for reading
     * @return Row object or null if no more rows
     * @throws HBaseAdapterException
     */
    public static Row nextRow(long scanId) throws HBaseAdapterException {
        try {
            long start = System.currentTimeMillis();
            ActiveScan conn = getActiveScanForId(scanId);
            HBaseResultScanner scanner = conn.getScanner();
            Row r = reader.nextRow(conn.getTableName(), scanner);
            long end = System.currentTimeMillis();
            Metrics.getInstance().addStat("Next row", end - start);
            return r;
        } catch (Throwable e) {
            logger.error("Exception:", e);
            throw new HBaseAdapterException("nextRow", e);
        }
    }

    /**
     * Ends a "session" for reading data and cleans up resources.
     *
     * @param scanId ID of the "session"
     * @throws HBaseAdapterException
     */
    public static void endScan(long scanId) throws HBaseAdapterException {
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

    /**
     * Writes a SQL row into HBase "SQL" table.
     *
     * @param writeId   The "session" ID for writing
     * @param tableName Name of SQL table
     * @param values    SQL row values
     * @return Success
     * @throws HBaseAdapterException
     */
    public static boolean writeRow(long writeId, String tableName, Map<String, byte[]> values)
            throws HBaseAdapterException {
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Using writer " + writeId);
            }
            long start = System.currentTimeMillis();
            HBaseWriter writer = getHBaseWriterForId(writeId);
            writer.writeRow(tableName, values);
            rowCountUpdate.set(true);
            long end = System.currentTimeMillis();
            Metrics.getInstance().addStat("Write Row", end - start);
        } catch (Exception e) {
            logger.error("Exception:", e);
            throw new HBaseAdapterException("writeRow", e);
        }

        return true;
    }

    /**
     * Updates a SQL row with new values.
     *
     * @param writeId       The "session" ID for writing
     * @param uuidBuff      The UUID (in a byte buffer) of the row to be updated
     * @param changedFields Columns that will change
     * @param tableName     Name of SQL table
     * @param values        SQL row values
     * @throws HBaseAdapterException
     */
    public static void updateRow(long writeId, byte[] uuidBuff, List<String> changedFields, String tableName, Map<String, byte[]> values)
            throws HBaseAdapterException {
        try {
            long start = System.currentTimeMillis();
            UUID uuid = Util.BytesToUUID(uuidBuff);
            HBaseWriter writer = getHBaseWriterForId(writeId);
            writer.updateRow(uuid, changedFields, tableName, values);
            rowCountUpdate.set(true);
            long end = System.currentTimeMillis();
            Metrics.getInstance().addStat("Update row", end - start);
        } catch (Exception e) {
            logger.error("Exception:", e);
            throw new HBaseAdapterException("writeRow", e);
        }
    }

    /**
     * Flush data stored in cache out to HBase
     *
     * @param writeId The "session" ID for writing
     */
    public static void flushWrites(long writeId) throws HBaseAdapterException {
        try {
            if (logger.isDebugEnabled()) {
                logger.debug(format("Flushing writer {0}", writeId));
            }
            long start = System.currentTimeMillis();
            HBaseWriter writer = getHBaseWriterForId(writeId);
            writer.flushWrites();
            long end = System.currentTimeMillis();
            Metrics.getInstance().addStat("Flush writes", end - start);
        } catch (Throwable e) {
            logger.error("Exception:", e);
            throw new HBaseAdapterException("Flush writes", e);
        }
    }

    /**
     * Deletes a SQL row out of HBase based on where the given scan
     * resides.
     *
     * @param tableName
     * @param uuidBuffer
     * @return success
     * @throws HBaseAdapterException
     */
    public static boolean deleteRow(String tableName, byte[] uuidBuffer) throws HBaseAdapterException {
        try {
            long start = System.currentTimeMillis();
            UUID uuid = Util.BytesToUUID(uuidBuffer);
            HBaseWriter writer = createWriter();
            boolean success = writer.deleteRow(tableName, uuid);
            writer.close();
            long end = System.currentTimeMillis();
            Metrics.getInstance().addStat("Delete Row", end - start);
            return success;
        } catch (Throwable e) {
            logger.error("Exception:", e);
            throw new HBaseAdapterException("deleteRow", e);
        }
    }

    /**
     * Remove all SQL rows from a SQL table
     *
     * @param tableName SQL table name
     * @return Number of rows removed
     * @throws HBaseAdapterException
     */
    public static int deleteAllRows(String tableName) throws HBaseAdapterException {
        try {
            long start = System.currentTimeMillis();
            HBaseWriter writer = createWriter();
            int r = writer.deleteAllRowsInTable(tableName);
            long end = System.currentTimeMillis();
            Metrics.getInstance().addStat("delete all rows", end - start);
            return r;
        } catch (Throwable e) {
            logger.error("Exception:", e);
            throw new HBaseAdapterException("deleteAllRowsInTable", e);
        }
    }

    /**
     * Drops a SQL table from HBase
     *
     * @param tableName SQL table name
     * @return success
     * @throws HBaseAdapterException
     */
    public static boolean dropTable(String tableName) throws HBaseAdapterException {
        try {
            long start = System.currentTimeMillis();
            HBaseWriter writer = createWriter();
            boolean b = writer.dropTable(tableName);
            long end = System.currentTimeMillis();
            Metrics.getInstance().addStat("Drop table", end - start);
            writer.close();
            return b;
        } catch (Throwable e) {
            logger.error("Exception:", e);
            throw new HBaseAdapterException("dropTable", e);
        }
    }

    /**
     * Retrieves a SQL row based on a unique identifier.
     *
     * @param scanId The "session" ID for reading
     * @param uuid   Data row unique identifier
     * @return SQL row
     * @throws HBaseAdapterException
     */
    public static Row getRow(long scanId, byte[] uuid) throws HBaseAdapterException {
        logger.debug(String.format("scanId: %d,%s", scanId, Bytes.toString(uuid)));
        try {
            long start = System.currentTimeMillis();
            ActiveScan activeScan = getActiveScanForId(scanId);
            String tableName = activeScan.getTableName();
            UUID rowUuid = Util.BytesToUUID(uuid);

            Row row = reader.getDataRow(rowUuid, tableName);
            long end = System.currentTimeMillis();
            Metrics.getInstance().addStat("get row", end - start);
            assert (row != null);

            return row;
        } catch (Throwable e) {
            logger.error("Exception:", e);
            throw new HBaseAdapterException("getRow", e);
        }

    }

    /**
     * Create a "session" for a index scan.
     *
     * @param tableName   SQL table name to scan
     * @param columnNames Columns to scan on
     * @return scan ID of the "session"
     * @throws HBaseAdapterException
     */
    public static long startIndexScan(String tableName, String columnNames)
            throws HBaseAdapterException {
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("tableName %s, columnNames: %s", tableName, columnNames));
        }
        try {
            long scanId = activeScanCounter.incrementAndGet();
            activeScanLookup.put(scanId, new ActiveScan(tableName, columnNames));
            return scanId;
        } catch (Throwable e) {
            logger.error("Exception:", e);
            throw new HBaseAdapterException("startIndexScan", e);
        }
    }

    /**
     * Checks that the SQL row would not violate a unique index on insert.
     *
     * @param tableName SQL table name
     * @param values    SQL row to check
     * @return Columns that violate unique index
     * @throws HBaseAdapterException
     */
    public static String findDuplicateKey(String tableName, Map<String, byte[]> values)
            throws HBaseAdapterException {
        try {
            long start = System.currentTimeMillis();
            String r = reader.findDuplicateKey(tableName, values);
            long end = System.currentTimeMillis();
            Metrics.getInstance().addStat("find dup key insert", end - start);
            return r;
        } catch (Throwable e) {
            logger.error("Exception:", e);
            throw new HBaseAdapterException("hasDuplicateValues", e);
        }
    }

    /**
     * Checks that the SQL row would not violate a unique index on update.
     *
     * @param tableName      SQL table name
     * @param values         SQL row to check
     * @param changedColumns Columns that will change on update
     * @return Columns that violate unique index
     * @throws HBaseAdapterException
     */
    public static String findDuplicateKey(String tableName, Map<String, byte[]> values, List<String> changedColumns)
            throws HBaseAdapterException {
        try {
            long start = System.currentTimeMillis();
            String r = reader.findDuplicateKeyOnUpdate(tableName, values, changedColumns);
            long end = System.currentTimeMillis();
            Metrics.getInstance().addStat("find dup key update", end - start);
            return r;
        } catch (Throwable e) {
            logger.error("Exception:", e);
            throw new HBaseAdapterException("hasDuplicateValues", e);
        }
    }

    /**
     * Checks that adding a unique index on an existing table would not have duplicates.
     *
     * @param tableName  SQL table name
     * @param columnName Columns used in index
     * @return A value that is duplicated in the table (null if no duplicates found)
     * @throws HBaseAdapterException
     */
    public static byte[] findDuplicateValue(String tableName, String columnName)
            throws HBaseAdapterException {
        try {
            long start = System.currentTimeMillis();
            byte[] r = reader.findDuplicateValue(tableName, columnName);
            long end = System.currentTimeMillis();
            Metrics.getInstance().addStat("find duplicate value", end - start);
            return r;
        } catch (Throwable e) {
            logger.error("Exception:", e);
            throw new HBaseAdapterException("columnContainsDuplicates", e);
        }
    }

    /**
     * Retrieve the next auto increment value from a SQL table.
     *
     * @param tableName  SQL table name
     * @param columnName Autoincrement column name
     * @return Next value of the autoincrement column
     * @throws HBaseAdapterException
     */
    public static long getNextAutoincrementValue(String tableName, String columnName)
            throws HBaseAdapterException {
        try {
            long start = System.currentTimeMillis();
            HBaseWriter writer = createWriter();
            long next = writer.getNextAutoincrementValue(tableName, columnName);
            writer.close();
            long end = System.currentTimeMillis();
            Metrics.getInstance().addStat("get next auto inc value", end - start);
            return next;
        } catch (Exception e) {
            logger.error("Exception:", e);
            throw new HBaseAdapterException("columnContainsDuplicates", e);
        }
    }

    /**
     * Retrieves a SQL row from the index and sets up a cursor into the index.
     * Returns null if no rows were found for the keys.
     *
     * @param scanId    The "session" ID for reading
     * @param keyValues Keys to search the index with
     * @param readType  Type of index scan
     * @return SQL row
     * @throws HBaseAdapterException
     */
    public static Row indexRead(long scanId, List<KeyValue> keyValues, IndexReadType readType)
            throws HBaseAdapterException {
        try {
            long start = System.currentTimeMillis();
            ActiveScan activeScan = getActiveScanForId(scanId);
            String tableName = activeScan.getTableName();
            List<String> columnName = activeScan.getColumnName();
            if (keyValues == null) {
                if (readType != IndexReadType.INDEX_FIRST
                        && readType != IndexReadType.INDEX_LAST) {
                    throw new IllegalArgumentException("keyValues can't be null unless first/last index read");
                }

                byte fill = (byte) (readType == IndexReadType.INDEX_FIRST ? 0x00 : 0xFF);
                keyValues = reader.setupKeyValues(tableName, columnName, fill);
            }

            ScanStrategyInfo scanInfo = new ScanStrategyInfo(tableName, columnName, keyValues);

            byte[] valueToSkip = null;
            HBaseResultScanner scanner;

            switch (readType) {
                case HA_READ_KEY_EXACT: {
                    ScanStrategy strategy = new PrefixScanStrategy(scanInfo);
                    scanner = new SingleResultScanner(reader.getScanner(strategy));
                }
                break;
                case HA_READ_AFTER_KEY: {
                    KeyValue lastKey = Iterables.getLast(scanInfo.keyValues());
                    ScanStrategy strategy = new OrderedScanStrategy(scanInfo);
                    if (lastKey.isNull()) {
                        scanner = new NonNullResultScanner(reader.getScanner(strategy));
                        valueToSkip = null;
                    } else {
                        scanner = new SingleResultScanner(reader.getScanner(strategy));
                        valueToSkip = Iterables.getLast(scanInfo.keyValueValues());
                    }

                }
                break;
                case HA_READ_KEY_OR_NEXT: {
                    ScanStrategy strategy = new OrderedScanStrategy(scanInfo);
                    scanner = new SingleResultScanner(reader.getScanner(strategy));
                }
                break;
                case HA_READ_BEFORE_KEY: {
                    ScanStrategy strategy = new ReverseScanStrategy(scanInfo);
                    scanner = new SingleResultScanner(reader.getScanner(strategy));
                    valueToSkip = Iterables.getLast(scanInfo.keyValueValues());
                }
                break;
                case HA_READ_KEY_OR_PREV: {
                    ScanStrategy strategy = new ReverseScanStrategy(scanInfo);
                    scanner = new SingleResultScanner(reader.getScanner(strategy));
                }
                break;
                case INDEX_FIRST: {
                    ScanStrategy strategy = new OrderedScanStrategy(scanInfo, true);
                    scanner = new SingleResultScanner(reader.getScanner(strategy));
                }
                break;
                case INDEX_LAST: {
                    ScanStrategy strategy = new ReverseScanStrategy(scanInfo, true);
                    scanner = new SingleResultScanner(reader.getScanner(strategy));
                }
                break;
                default:
                    throw new IllegalStateException("Not a valid MySQL scan type " + readType);
            }

            scanner.setColumnName(Iterables.getLast(scanInfo.keyValueColumns()));

            activeScan.setScanner(scanner);

            Result result = scanner.next(valueToSkip);


            if (result == null) {
                long end = System.currentTimeMillis();
                Metrics.getInstance().addStat("index read", end - start);
                return null;
            }

            Row r = ResultReader.readIndexRow(result);
            long end = System.currentTimeMillis();
            Metrics.getInstance().addStat("index read", end - start);
            return r;
        } catch (Throwable e) {
            logger.error("Exception:", e);
            throw new HBaseAdapterException("indexRead", e);
        }
    }

    /**
     * Retrieves a SQL row from the index and moves the index cursor forward.
     * Returns null if there are no more rows.
     *
     * @param scanId The "session" ID for reading
     * @return SQL row
     * @throws HBaseAdapterException
     */
    public static Row nextIndexRow(long scanId) throws HBaseAdapterException {
        try {
            long start = System.currentTimeMillis();
            ActiveScan conn = getActiveScanForId(scanId);
            HBaseResultScanner scanner = conn.getScanner();
            Result result = scanner.next(null);
            if (result == null) {
                long end = System.currentTimeMillis();
                Metrics.getInstance().addStat("next index row", end - start);
                return null;
            }

            Row r = ResultReader.readIndexRow(result);
            long end = System.currentTimeMillis();
            Metrics.getInstance().addStat("next index row", end - start);
            return r;
        } catch (Throwable e) {
            logger.error("Exception:", e);
            throw new HBaseAdapterException("nextIndexRow", e);
        }
    }

    /**
     * Increment the SQL row count in HBase.
     *
     * @param tableName SQL table name
     * @param delta     Amount to increment
     * @throws HBaseAdapterException
     */
    public static void incrementRowCount(String tableName, long delta) throws HBaseAdapterException {
        try {
            long start = System.currentTimeMillis();
            HBaseWriter writer = createWriter();
            writer.incrementRowCount(tableName, delta);
            rowCountCache.incrementAndGet();
            writer.close();
            long end = System.currentTimeMillis();
            Metrics.getInstance().addStat("increment row count", end - start);
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw new HBaseAdapterException("incrementRowCount", e);
        }
    }

    /**
     * Set the SQL row count in HBase.
     *
     * @param tableName SQL table name
     * @param count     Value to set the count to
     * @throws HBaseAdapterException
     */
    public static void setRowCount(String tableName, long count) throws HBaseAdapterException {
        try {
            HBaseWriter writer = createWriter();
            rowCountCache.getAndSet(count);
            writer.setRowCount(tableName, count);
            writer.close();
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw new HBaseAdapterException("setRowCount", e);
        }
    }

    /**
     * Retrieve the SQL row count in HBase
     *
     * @param tableName SQL table name
     * @return SQL row count
     * @throws HBaseAdapterException
     */
    public static long getRowCount(String tableName) throws HBaseAdapterException {
        try {
            if (rowCountUpdate.compareAndSet(true, false)) {
                rowCountCache.set(reader.getRowCount(tableName));
                return rowCountCache.get();
            }

            return rowCountCache.get();
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw new HBaseAdapterException("getRowCount", e);
        }
    }

    /**
     * Renames a SQL table in HBase
     *
     * @param from Old SQL table name
     * @param to   New SQL table name
     * @throws HBaseAdapterException
     */
    public static void renameTable(String from, String to) throws HBaseAdapterException {
        try {
            HBaseWriter writer = createWriter();
            writer.renameTable(from, to);
            writer.close();
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw new HBaseAdapterException("renameTable", e);
        }
    }

    /**
     * Query whether a column on a SQL table is NULL
     *
     * @param tableName  SQL table name
     * @param columnName Column name in question
     * @return Whether the column is nullable
     * @throws HBaseAdapterException
     */
    public static boolean isNullable(String tableName, String columnName) throws HBaseAdapterException {
        try {
            return reader.isNullable(tableName, columnName);
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw new HBaseAdapterException("isNullable", e);
        }
    }

    /**
     * Adds an index to a SQL table
     *
     * @param tableName      SQL table name
     * @param columnsToIndex SQL table columns used in the index
     * @throws HBaseAdapterException
     */
    public static void addIndex(String tableName, TableMultipartKeys columnsToIndex) throws HBaseAdapterException {
        try {
            long start = System.currentTimeMillis();
            HBaseWriter writer = createWriter();
            writer.addIndex(tableName, columnsToIndex);
            writer.close();
            long end = System.currentTimeMillis();
            Metrics.getInstance().addStat("add index", end - start);
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw new HBaseAdapterException("addIndex", e);
        }
    }

    /**
     * Remove an index from a SQL table
     *
     * @param tableName   SQL table name
     * @param indexToDrop SQL table columns used in the index
     * @throws HBaseAdapterException
     */
    public static void dropIndex(String tableName, String indexToDrop) throws HBaseAdapterException {
        try {
            long start = System.currentTimeMillis();
            HBaseWriter writer = createWriter();
            writer.dropIndex(tableName, indexToDrop);
            writer.close();
            long end = System.currentTimeMillis();
            Metrics.getInstance().addStat("drop index", end - start);
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw new HBaseAdapterException("dropIndex", e);
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

        try {
            params = Util.readConfiguration(configFile);
        } catch (ParserConfigurationException e) {
            logger.fatal("The xml parser was not configured properly.", e);
        } catch (SAXException e) {
            logger.fatal("Exception while trying to parse the config file.", e);
        }

        logger.info(format("Read in {0} parameters.", params.size()));

        try {
            String tableName = params.get(Constants.HBASE_TABLE),
                    zkQuorum = params.get("zk_quorum");
            writeBufferSize = params.getLong("write_buffer_size", DEFAULT_WRITE_BUFFER_SIZE);
            int poolSize = params.getInt("honeycomb.pool_size", DEFAULT_TABLE_POOL_SIZE);
            boolean autoFlush = params.getBoolean("flush_changes_immediately", false);

            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", zkQuorum);
            configuration.set(Constants.HBASE_TABLE, tableName);
            SqlTableCreator.initializeSqlTable(configuration);
            tablePool = new HTablePool(configuration, poolSize, new HTableFactory(writeBufferSize, autoFlush));
            HTableInterface readerTable = tablePool.getTable(tableName);
            reader = new HBaseReader(readerTable);
        } catch (ZooKeeperConnectionException e) {
            logger.fatal("Could not connect to zookeeper. ", e);
            throw e;
        } catch (IOException e) {
            logger.fatal("Could not create HBase client. Aborting initialization.");
            throw e;
        }

        reader.setCacheSize(params.getInt("table_scan_cache_rows", DEFAULT_NUM_CACHED_ROWS));

        MBeans.register("Honeycomb", "Statistics", Metrics.getInstance());

        isInitialized = true;
        logger.info("End");
    }

    private static HBaseWriter createWriter() {
        long start = System.currentTimeMillis();
        HTableInterface table = tablePool.getTable(params.get(Constants.HBASE_TABLE));
        HBaseWriter w = new HBaseWriter(table);
        w.setWriteBufferSize(writeBufferSize);
        long end = System.currentTimeMillis();
        Metrics.getInstance().addStat("create writer", end - start);
        return w;
    }

    private static HBaseWriter getHBaseWriterForId(long writeId) throws HBaseAdapterException {
        HBaseWriter writer = activeWriterLookup.get(writeId);
        if (writer == null) {
            throw new HBaseAdapterException("No connection for scanId: " + writeId, null);
        }
        return writer;
    }

    private static ActiveScan getActiveScanForId(long scanId) throws HBaseAdapterException {
        ActiveScan conn = activeScanLookup.get(scanId);
        if (conn == null) {
            throw new HBaseAdapterException("No connection for scanId: " + scanId, null);
        }
        return conn;
    }
}
