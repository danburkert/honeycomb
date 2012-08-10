package com.nearinfinity.mysqlengine.jni;

import com.nearinfinity.mysqlengine.Connection;
import com.nearinfinity.mysqlengine.HBaseClient;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.log4j.Logger;

import java.io.*;
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

            //Initiliaze class variables
            client = new HBaseClient(params.get("hbase_table_name"), params.get("zk_quorum"));
            connectionCounter = new AtomicLong(0L);
            clientPool = new ConcurrentHashMap<Long, Connection>();

            //We are now configured
            configured = true;
        }
        catch (FileNotFoundException e) {
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
        }
        catch (Exception e) {
            logger.error("Exception in createTable", e);
            throw new HBaseAdapterException("createTable", e);
        }

        return true;
    }

    public static long startScan(String tableName) throws HBaseAdapterException {
        logger.info("Starting scan on table " + tableName);

        long scanId = connectionCounter.incrementAndGet();
        try {
            ResultScanner scanner = client.getTableScanner(tableName);
            clientPool.put(scanId, new Connection(tableName, scanner));
        }
        catch (IOException e) {
            logger.error("Exception in startScan ", e);
            throw new HBaseAdapterException("startScan", e);
        }

        return scanId;
    }

    public static Row nextRow(long scanId) throws HBaseAdapterException {
        logger.info("nextRow:  scanId: " + scanId);

        Connection conn = getConnectionForId(scanId);

        Row row = new Row();
        try {

            //Return empty Row if there is no next result
            Result result = conn.getNextResult();
            if (result == null) {
                return row;
            }

            //Set values and UUID
            Map<String, byte[]> values = client.parseRow(result, conn.getTableName());
            UUID uuid = client.parseUUIDFromDataRow(result);
            row.setRowMap(values);
            row.setUUID(uuid);
        }
        catch  (Exception e) {
            logger.error("Exception thrown in nextRow", e);
            throw new HBaseAdapterException("nextRow", e);
        }

        return row;
    }

    public static Row[] nextRows(long scanId, long numRows) throws HBaseAdapterException {
        logger.info("Getting " + numRows + " rows using scanId " + scanId);

        Connection conn = getConnectionForId(scanId);

        ArrayList<Row> rowList = new ArrayList<Row>();
        try {

            for (long i = 0 ; i < numRows ; i++) {
                Result result = conn.getNextResult();
                if (result == null) {
                    return (Row[])rowList.toArray();
                }
                Map<String, byte[]> values = client.parseRow(result, conn.getTableName());
                UUID uuid = client.parseUUIDFromDataRow(result);

                rowList.add(new Row(values, uuid));
            }
        }
        catch (Exception e) {
            logger.error("Exception thrown in nextRows", e);
            throw new HBaseAdapterException("nextRows", e);
        }

        return (Row[])rowList.toArray();
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
        }
        catch (Exception e) {
            logger.error("Exception thrown in writeRow()", e);
            throw new HBaseAdapterException("writeRow", e);
        }

        return true;
    }

    public static boolean deleteRow(long scanId) throws HBaseAdapterException {
        logger.info("Deleting row with scanId " + scanId);

        boolean deleted;
        try {
            Connection conn = clientPool.get(scanId);
            Result result = conn.getLastResult();
            byte[] rowKey = result.getRow();

            deleted = client.deleteRow(rowKey);
        }
        catch (IOException e) {
            logger.error("Exception thrown in deleteRow()", e);
            throw new HBaseAdapterException("deleteRow", e);
        }

        return deleted;
    }

    public static Row getRow(long scanId, String tableName /*TODO: Can we delete this? */, byte[] uuid)  throws HBaseAdapterException {
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
