package com.nearinfinity.mysqlengine.jni;

import com.nearinfinity.mysqlengine.Connection;
import com.nearinfinity.mysqlengine.DataConnection;
import com.nearinfinity.mysqlengine.HBaseClient;
import com.nearinfinity.mysqlengine.IndexConnection;
import com.sun.jersey.api.NotFoundException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.log4j.Logger;
import org.omg.CosNaming.NamingContextPackage.NotFound;

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
            logger.info("Static Initializer-> Begin");

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

            } catch (NumberFormatException e) {
                logger.info("Static Initializer-> Number of rows to cache was not provided or invalid" +
                        " - using default of " + DEFAULT_NUM_CACHED_ROWS);
                client.setCacheSize(DEFAULT_NUM_CACHED_ROWS);
            }

            try {
                long writeBufferSize = Long.parseLong(params.get("write_buffer_size"));
                client.setWriteBufferSize(writeBufferSize);
            } catch (NumberFormatException e) {
                logger.info("Static Initializer-> Write buffer size was not provided or invalid - using default of " + DEFAULT_WRITE_BUFFER_SIZE);
                client.setWriteBufferSize(DEFAULT_WRITE_BUFFER_SIZE);
            }

            boolean flushChangesImmediately = Boolean.parseBoolean(params.get("flush_changes_immediately"));
            client.setAutoFlushTables(flushChangesImmediately);

            //We are now configured
            configured = true;
        } catch (FileNotFoundException e) {
            logger.warn("Static Initializer-> FileNotFoundException:", e);
        }
    }

    public static boolean isConfigured() {
        return configured;
    }

    public static boolean createTable(String tableName, List<String> columnNames) throws HBaseAdapterException {
        logger.info("creatingTable-> tableName:" + tableName + ", columnName: " + columnNames.toString());

        try {
            client.createTableFull(tableName, columnNames);
        } catch (Exception e) {
            logger.error("createTable-> Exception:", e);
            throw new HBaseAdapterException("createTable", e);
        }

        return true;
    }

    public static long startScan(String tableName, boolean isFullTableScan) throws HBaseAdapterException {
        long scanId = connectionCounter.incrementAndGet();
        logger.info("startScan-> tableName: " + tableName + ", scanId: " + scanId);
        try {
            ResultScanner scanner = client.getTableScanner(tableName, isFullTableScan);
            clientPool.put(scanId, new DataConnection(tableName, scanner));
        } catch (Exception e) {
            logger.error("startScan-> Exception:", e);
            throw new HBaseAdapterException("startScan", e);
        }

        return scanId;
    }

    public static long startIndexScan(String tableName, String columnName) throws HBaseAdapterException {
        logger.info("startIndexScan-> tableName " + tableName + ", columnName: " + columnName);

        long scanId = connectionCounter.incrementAndGet();
        try {
            clientPool.put(scanId, new IndexConnection(tableName, columnName));
        } catch (Exception e) {
            logger.error("startIndexScan-> Exception:", e);
            throw new HBaseAdapterException("startIndexScan", e);
        }

        return scanId;
    }

    public static Row nextRow(long scanId) throws HBaseAdapterException {
        logger.info("nextRow-> scanId: " + scanId);

        Connection conn = getConnectionForId(scanId);
        long start = System.currentTimeMillis();

        Row row = new Row();

        try {
            Result result = conn.getNextResult();
            if (result == null) {
                return null;
            }

            //Set values and UUID
            Map<String, byte[]> values = client.parseRow(result, conn.getTableName());
            UUID uuid = client.parseUUIDFromDataRow(result);
            row.setRowMap(values);
            row.setUUID(uuid);
            logger.info("\t\t UUID: " + uuid.toString());

        } catch (Exception e) {
            logger.error("nextRow-> Exception:", e);
            throw new HBaseAdapterException("nextRow", e);
        }

        elapsedTime += System.currentTimeMillis() - start;
        return row;
    }

    public static void endScan(long scanId) throws HBaseAdapterException {
        logger.info("endScan-> scanId: " + scanId);
        Connection conn = getConnectionForId(scanId);
        conn.close();
    }

    public static boolean writeRow(String tableName, Map<String, byte[]> values, byte[] unireg) throws HBaseAdapterException {
        logger.info("writeRow-> tableName: " + tableName);

        try {
            client.writeRow(tableName, values, unireg);
        } catch (Exception e) {
            logger.error("writeRow-> Exception:", e);
            throw new HBaseAdapterException("writeRow", e);
        }

        return true;
    }

    public static void flushWrites() {
        client.flushWrites();
    }

    public static boolean deleteRow(long scanId) throws HBaseAdapterException {
        logger.info("deleteRow-> scanId: " + scanId);

        boolean deleted;
        try {
            Connection conn = clientPool.get(scanId);
            Result result = conn.getLastResult();
            byte[] rowKey = result.getRow();

            deleted = client.deleteRow(rowKey);
        } catch (IOException e) {
            logger.error("deleteRow-> Exception:", e);
            throw new HBaseAdapterException("deleteRow", e);
        }

        return deleted;
    }

    public static int deleteAllRows(String tableName) throws HBaseAdapterException {
        logger.info("deleteAllRows-> tableName: " + tableName);

        int deleted;
        try {
            deleted = client.deleteAllRows(tableName);
        } catch (IOException e) {
            logger.error("deleteAllRows-> Exception:", e);
            throw new HBaseAdapterException("deleteAllRows", e);
        }

        return deleted;
    }

    public static boolean dropTable(String tableName) throws HBaseAdapterException {
        logger.info("dropTable-> tableName: " + tableName);

        boolean deleted;
        try {
            deleted = client.dropTable(tableName);
        } catch (IOException e) {
            logger.error("dropTable-> Exception:", e);
            throw new HBaseAdapterException("dropTable", e);
        }

        return deleted;
    }

    public static Row getRow(long scanId, byte[] uuid) throws HBaseAdapterException {
        logger.info("getRow-> scanId: " + scanId + ",");
        Connection conn = getConnectionForId(scanId);

        Row row = new Row();
        try {
            String tableName = conn.getTableName();
            ByteBuffer buffer = ByteBuffer.wrap(uuid);
            UUID rowUuid = new UUID(buffer.getLong(), buffer.getLong());
            logger.info("\t\t UUID: " + rowUuid.toString());

            Result result = client.getDataRow(rowUuid, tableName);
            if (result == null) {
                logger.error("getRow-> Exception: Row not found");
                throw new HBaseAdapterException("getRow", new NotFoundException());
            }

            Map<String, byte[]> values = client.parseRow(result, conn.getTableName());
            logger.info("\t\t values.size: " + values.size());
            row.setUUID(rowUuid);
            row.setRowMap(values);
        } catch (Exception e) {
            logger.error("getRow-> Exception:", e);
            throw new HBaseAdapterException("getRow", e);
        }

        return row;
    }

    public static byte[] indexRead(long scanId, byte[] value, IndexReadType readType) throws HBaseAdapterException {
        logger.info("indexRead-> scanId: " + scanId + ", readType: " + readType.name());
        StringBuilder sb = new StringBuilder();
        for (byte b : value) {
            sb.append(String.format("%02X ", b));
        }
        logger.info("\t\tIndex key: " + sb.toString());
        IndexConnection conn = (IndexConnection) getConnectionForId(scanId);

        byte[] unireg = null;
        try {
            String tableName = conn.getTableName();
            String columnName = conn.getColumnName();
            logger.info("\t\ttableName: " + tableName + ", columnName " + columnName);

            conn.setReadType(readType);

            switch (readType) {
                case HA_READ_KEY_EXACT: {
                    ResultScanner indexScanner = client.getSecondaryIndexScannerExact(tableName, columnName, value);
                    conn.setIndexScanner(indexScanner);

                    //Get the first row of the value
                    Result result = conn.getNextIndexResult();
                    if (result == null) {
                        return unireg;
                    }

                    ResultScanner scanner = client.getValueIndexScanner(tableName, columnName, value);
                    conn.setScanner(scanner);
                    //Get the first result to return
                    Result valueIndexResult = conn.getNextResult();
                    if (valueIndexResult == null) {
                        return unireg;
                    }
                    unireg = client.parseUniregFromIndex(result);
                }
                break;
                case HA_READ_AFTER_KEY: {
                    ResultScanner indexScanner = client.getSecondaryIndexScanner(tableName, columnName, value);
                    conn.setIndexScanner(indexScanner);

                    //Get the first row of the value
                    Result result = conn.getNextIndexResult();
                    if (result == null) {
                        return unireg;
                    }

                    byte[] indexValue = client.parseValueFromSecondaryIndexRow(result);
                    if (Arrays.equals(value, indexValue)) {
                        //Get the next index result
                        Result nextResult = conn.getNextIndexResult();
                        if (nextResult == null) {
                            return unireg;
                        }
                    }

                    ResultScanner scanner = client.getValueIndexScanner(tableName, columnName, value);
                    conn.setScanner(scanner);
                    //Get the first result to return
                    Result valueIndexResult = conn.getNextResult();
                    if (valueIndexResult == null) {
                        return unireg;
                    }
                    unireg = client.parseUniregFromIndex(result);
                }
                break;
                case HA_READ_KEY_OR_NEXT: {
                    ResultScanner indexScanner = client.getSecondaryIndexScanner(tableName, columnName, value);
                    conn.setIndexScanner(indexScanner);

                    //Get the first row of the value
                    Result result = conn.getNextIndexResult();
                    if (result == null) {
                        return unireg;
                    }

                    ResultScanner scanner = client.getValueIndexScanner(tableName, columnName, value);
                    conn.setScanner(scanner);
                    //Get the first result to return
                    Result valueIndexResult = conn.getNextResult();
                    if (valueIndexResult == null) {
                        return unireg;
                    }
                    unireg = client.parseUniregFromIndex(result);
                }
                break;
                case HA_READ_BEFORE_KEY: {
                    ResultScanner indexScanner = client.getReverseIndexScanner(tableName, columnName, value);
                    conn.setIndexScanner(indexScanner);

                    //Get the first row of the value
                    Result result = conn.getNextIndexResult();
                    if (result == null) {
                        return unireg;
                    }

                    byte[] indexValue = client.parseValueFromReverseIndexRow(result);
                    if (Arrays.equals(value, indexValue)) {
                        //Get the next index result
                        Result nextResult = conn.getNextIndexResult();
                        if (nextResult == null) {
                            return unireg;
                        }
                    }

                    ResultScanner scanner = client.getValueIndexScanner(tableName, columnName, value);
                    conn.setScanner(scanner);
                    //Get the first result to return
                    Result valueIndexResult = conn.getNextResult();
                    if (valueIndexResult == null) {
                        return unireg;
                    }
                    unireg = client.parseUniregFromIndex(result);
                }
                break;
                case HA_READ_KEY_OR_PREV: {
                    ResultScanner indexScanner = client.getReverseIndexScanner(tableName, columnName, value);
                    conn.setIndexScanner(indexScanner);

                    //Get the first row of the value
                    Result result = conn.getNextIndexResult();
                    if (result == null) {
                        return unireg;
                    }

                    ResultScanner scanner = client.getValueIndexScanner(tableName, columnName, value);
                    conn.setScanner(scanner);

                    //Get the first result to return
                    Result valueIndexResult = conn.getNextResult();
                    if (valueIndexResult == null) {
                        return unireg;
                    }
                    unireg = client.parseUniregFromIndex(result);
                }
                break;
            }
        } catch (Exception e) {
            logger.error("indexRead-> Exception:", e);
            throw new HBaseAdapterException("indexRead", e);
        }

        return unireg;
    }

    public static byte[] nextIndexRow(long scanId) throws HBaseAdapterException {
        logger.info("nextIndexRow-> scanId: " + scanId);

        IndexConnection conn = (IndexConnection) getConnectionForId(scanId);
        long start = System.currentTimeMillis();

        byte[] unireg = null;
        try {
            String tableName = conn.getTableName();
            String columnName = conn.getColumnName();

            Result result = conn.getNextResult();
            while (result == null) {
                //Get the first row of the value
                Result indexResult = conn.getNextIndexResult();
                if (indexResult == null) {
                    return unireg;
                }

                byte[] value = null;
                switch (conn.getReadType()) {
                    case HA_READ_AFTER_KEY:
                    case HA_READ_KEY_OR_NEXT: {
                        value = client.parseValueFromSecondaryIndexRow(indexResult);
                    }
                    break;
                    case HA_READ_BEFORE_KEY:
                    case HA_READ_KEY_OR_PREV: {
                        value = client.parseValueFromReverseIndexRow(indexResult);
                    }
                }

                if (value == null) {
                    return unireg;
                }

                ResultScanner scanner = client.getValueIndexScanner(tableName, columnName, value);
                conn.setScanner(scanner);

                //Get the next result, let the loop determine if we need to loop again
                result = conn.getNextResult();
            }

            unireg = client.parseUniregFromIndex(result);
        } catch (Exception e) {
            logger.error("nextIndexRow-> Exception:", e);
            throw new HBaseAdapterException("nextIndexRow", e);
        }

        elapsedTime += System.currentTimeMillis() - start;
        return unireg;
    }

    private static Connection getConnectionForId(long scanId) throws HBaseAdapterException {
        //logger.info("getConnectionForId-> scanId: " + scanId); // Obnoxious
        Connection conn = clientPool.get(scanId);
        if (conn == null) {
            throw new HBaseAdapterException("getConnectionForId->No connection for scanId: " + scanId, null);
        }
        return conn;
    }
}
