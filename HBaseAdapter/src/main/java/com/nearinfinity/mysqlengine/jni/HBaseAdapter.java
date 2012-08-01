package com.nearinfinity.mysqlengine.jni;

import com.nearinfinity.mysqlengine.Connection;
import com.nearinfinity.mysqlengine.HBaseClient;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;
import java.util.List;
import java.util.Map;
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
    private static final String HBASE_TABLE_NAME = "sql";
    private static AtomicLong connectionCounter = new AtomicLong(0L);
    private static Map<Long, Connection> clientPool = new ConcurrentHashMap<Long, Connection>();
    private static HBaseClient client = new HBaseClient(HBASE_TABLE_NAME);

    public static boolean createTable(String tableName, List<String> columnNames) throws HBaseAdapterException {
        try {
            client.createTableFull(tableName, columnNames);
        }
        catch (IOException e) {
            throw new HBaseAdapterException("IOException", e.toString());
        }
        return true;
    }

    public static long startScan(String tableName) throws HBaseAdapterException {
        long scanId = connectionCounter.incrementAndGet();
        try {
            ResultScanner scanner = client.getTableScanner(tableName);
            clientPool.put(scanId, new Connection(tableName, scanner));
        }
        catch (IOException e) {
            throw new HBaseAdapterException("IOException", e.toString());
        }
        return scanId;
    }

    public static Row nextRow(long scanId) throws HBaseAdapterException {
        Connection conn = clientPool.get(scanId);
        if (conn == null) {
            throw new HBaseAdapterException("Cannot find scanId key", "");
        }
        Row row = new Row();
        try {
            Result result = conn.getScanner().next();
            if (result == null) {
                return row;
            }
            Map<String, byte[]> values = client.parseRow(result, conn.getTableName());
            row.setValues(values);
        }
        catch  (IOException e) {
            throw new HBaseAdapterException("IOException", e.toString());
        }
        return row;
    }

    public static void endScan(long scanId) throws HBaseAdapterException {
        Connection conn = clientPool.remove(scanId);
        if (conn == null) {
            throw new HBaseAdapterException("ScanId does not exist", "");
        }
    }

    public static boolean writeRow(String tableName, Map<String, byte[]> values) throws HBaseAdapterException {
        try {
            client.writeRow(tableName, values);
        }
        catch (IOException e) {
            throw new HBaseAdapterException("IOException", e.toString());
        }
        return true;
    }
}
