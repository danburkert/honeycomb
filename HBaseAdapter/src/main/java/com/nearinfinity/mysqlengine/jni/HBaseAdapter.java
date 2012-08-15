package com.nearinfinity.mysqlengine.jni;

import com.nearinfinity.mysqlengine.Connection;
import com.nearinfinity.mysqlengine.HBaseClient;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created with IntelliJ IDEA.
 * User: jedstrom
 * Date: 8/1/12
 * Time: 9:46 AM
 * To change this template use File | Settings | File Templates.
 */
public class HBaseAdapter {
    private static AtomicLong connectionCounter;
    private static Map<Long, Connection> clientPool;
    private static HBaseClient client;
    private static boolean configured;
    private static final Logger logger = Logger.getLogger(HBaseAdapter.class);

    private static final int DEFAULT_NUM_CACHED_ROWS = 2500;
    private static final long DEFAULT_WRITE_BUFFER_SIZE = 5 * 1024 * 1024; // 5 megabytes
    private static long elapsedTime;

    static {
        try {
            logger.info("Static initializer");

            //Read config options from adapter.conf
            Scanner confFile = new Scanner(new File("/etc/mysql/adapter.conf"));
            Map<String, String> params = new HashMap<String, String>();
            while (confFile.hasNextLine()) {
                Scanner line = new Scanner(confFile.nextLine());
                params.put(line.next(), line.next());
            }

            //Initialize class variables
            client = new HBaseClient(params.get("hbase_table_name"), params.get("zk_quorum"));
            connectionCounter = new AtomicLong(0L);
            clientPool = new ConcurrentHashMap<Long, Connection>();

            try {
                int cacheSize = Integer.parseInt(params.get("table_scan_cache_rows"));
                client.setCacheSize(cacheSize);

            }
            catch (NumberFormatException e) {
                logger.info("Number of rows to cache was not provided or invalid - using default of " + DEFAULT_NUM_CACHED_ROWS);
                client.setCacheSize(DEFAULT_NUM_CACHED_ROWS);
            }

            try {
                long writeBufferSize = Long.parseLong(params.get("write_buffer_size"));
                client.setWriteBufferSize(writeBufferSize);
            }
            catch (NumberFormatException e) {
                logger.info("Write buffer size was not provided or invalid - using default of " + DEFAULT_WRITE_BUFFER_SIZE);
                client.setWriteBufferSize(DEFAULT_WRITE_BUFFER_SIZE);
            }

            boolean flushChangesImmediately = Boolean.parseBoolean(params.get("flush_changes_immediately"));
            client.setAutoFlushTables(flushChangesImmediately);

            //We are now configured
            configured = true;
        } catch (FileNotFoundException e) {
            logger.warn("FileNotFoundException", e);
        }
    }

    public static boolean isConfigured() {
        return configured;
    }

    public static boolean createTable(String tableName, List<String> columnNames) throws HBaseAdapterException {
        logger.info("Creating table " + tableName);

        try {
            client.createTableFull(tableName, columnNames);
        } catch (Exception e) {
            logger.error("Exception in createTable", e);
            throw new HBaseAdapterException("createTable", e);
        }

        return true;
    }

    public static long startScan(String tableName, boolean isFullTableScan) throws HBaseAdapterException {
        logger.info("Starting scan on table " + tableName);

        long scanId = connectionCounter.incrementAndGet();
        logger.info("Starting scan: scanId " + scanId);
        try {
            ResultScanner scanner = client.getTableScanner(tableName, isFullTableScan);
            clientPool.put(scanId, new Connection(tableName, scanner));
        } catch (Exception e) {
            logger.error("Exception in startScan ", e);
            throw new HBaseAdapterException("startScan", e);
        }

        return scanId;
    }

    public static long startIndexScan(String tableName, String columnName) throws HBaseAdapterException {
        logger.info("Starting index scan on table " + tableName);

        long scanId = connectionCounter.incrementAndGet();
        try {
            ResultScanner scanner = client.getIndexScanner(tableName, columnName);
            clientPool.put(scanId, new Connection(tableName, scanner));
        } catch (Exception e) {
            logger.error("Exception in startIndexScan ", e);
            throw new HBaseAdapterException("startIndexScan", e);
        }

        return scanId;
    }

    public static Row nextRow(long scanId) throws HBaseAdapterException {
        logger.info("nextRow:  scanId: " + scanId);

        Connection conn = getConnectionForId(scanId);
        long start = System.currentTimeMillis();

        Row row = new Row();

        try {
            Result result = conn.getNextResult();
            if (result == null) {
                return row;
            }

            //Set values and UUID
            Map<String, byte[]> values = client.parseRow(result, conn.getTableName());
            UUID uuid = client.parseUUIDFromDataRow(result);
            row.setRowMap(values);
            row.setUUID(uuid);
        } catch (Exception e) {
            logger.error("Exception thrown in nextRow", e);
            throw new HBaseAdapterException("nextRow", e);
        }

        elapsedTime += System.currentTimeMillis() - start;
        return row;
    }

    public static void endScan(long scanId) throws HBaseAdapterException {
        logger.info("Ending scan with id " + scanId);
        Connection conn = getConnectionForId(scanId);
        conn.close();
    }

    public static boolean writeRow(String tableName, Map<String, byte[]> values) throws HBaseAdapterException {
        logger.info("Writing row to table " + tableName);

        try {
            client.writeRow(tableName, values);
        } catch (Exception e) {
            logger.error("Exception thrown in writeRow()", e);
            throw new HBaseAdapterException("writeRow", e);
        }

        return true;
    }

    public static boolean writeRow(String tableName, Map<String, byte[]>[] values) throws HBaseAdapterException {
        logger.info("HBaseAdapter: Writing row to table " + tableName);
        try {
            client.writeRow(tableName, values);
        } catch (Exception e) {
            logger.error("During bulk insert", e);
            throw new HBaseAdapterException("IOException", e);
        }
        return true;
    }

    public static void flushWrites() {
        client.flushWrites();
    }


    public static boolean deleteRow(long scanId) throws HBaseAdapterException {
        logger.info("Deleting row with scanId " + scanId);

        boolean deleted;
        try {
            Connection conn = clientPool.get(scanId);
            Result result = conn.getLastResult();
            byte[] rowKey = result.getRow();

            deleted = client.deleteRow(rowKey);
        } catch (IOException e) {
            logger.error("Exception thrown in deleteRow()", e);
            throw new HBaseAdapterException("deleteRow", e);
        }

        return deleted;
    }

    public static int deleteAllRows(String tableName) throws HBaseAdapterException {
        logger.info("Deleting all rows from table " + tableName);

        int deleted;
        try {
            deleted = client.deleteAllRows(tableName);
        } catch (IOException e) {
            logger.error("Exception thrown in deleteAllRows()",  e);
            throw new HBaseAdapterException("deleteAllRows", e);
        }

        return deleted;
    }

    public static boolean dropTable(String tableName) throws HBaseAdapterException {
        logger.info("Dropping table " + tableName);

        boolean deleted;
        try {
            deleted = client.dropTable(tableName);
        } catch (IOException e) {
            logger.error("Exception thrown in dropTable()", e);
            throw new HBaseAdapterException("dropTable", e);
        }

        return deleted;
    }

    public static Row getRow(long scanId, String tableName /*TODO: Can we delete this? */, byte[] uuid) throws HBaseAdapterException {
        logger.info("Getting row with scanId " + scanId);

        Connection conn = getConnectionForId(scanId);

        Row row = new Row();
        try {
            //String tableName = conn.getTableName();
            ByteBuffer buffer = ByteBuffer.wrap(uuid);
            UUID rowUuid = new UUID(buffer.getLong(), buffer.getLong());
            logger.info("Getting row with UUID: " + rowUuid.toString() + ", and scanId: " + scanId);

            Result result = client.getDataRow(rowUuid, tableName);
            if (result == null) {
                return row;
            }

            Map<String, byte[]> values = client.parseRow(result, conn.getTableName());
            row.setUUID(rowUuid);
            row.setRowMap(values);
        } catch (Exception e) {
            logger.error("Exception thrown in getRow()", e);
            throw new HBaseAdapterException("getRow", e);
        }

        return row;
    }

    public static Row getRowByValue(long scanId, String columnName, byte[] value) throws HBaseAdapterException {
        logger.info("Getting row by value with scanId " + scanId + " for column " + columnName);

        Connection conn = getConnectionForId(scanId);

        Row row = new Row();
        try {
            String tableName = conn.getTableName();

        }
        catch (Exception e) {
            logger.error("Exception thrown in getRow()", e);
            throw new HBaseAdapterException("getRow", e);
        }

        return row;
    }

    private static Connection getConnectionForId(long scanId) throws HBaseAdapterException {
        Connection conn = clientPool.get(scanId);
        if (conn == null) {
            throw new HBaseAdapterException("No connection for scanId " + scanId, null);
        }
        return conn;
    }
}
