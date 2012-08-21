package com.nearinfinity.mysqlengine.readstrategies;

import com.nearinfinity.mysqlengine.HBaseClient;
import com.nearinfinity.mysqlengine.IndexConnection;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: showell
 * Date: 8/20/12
 * Time: 4:35 PM
 * To change this template use File | Settings | File Templates.
 */
public class NullReadStrategy implements IndexReadStrategy {
    private final Index client;

    public NullReadStrategy(Index client) {
        this.client = client;
    }

    @Override
    public void setupResultScannerForConnection(IndexConnection conn, byte[] value) throws IOException {
        String tableName = conn.getTableName();
        String columnName = conn.getColumnName();
        conn.setNullScan(true);

        ResultScanner nullScanner = client.getNullIndexScanner(tableName, columnName);
        conn.setIndexScanner(nullScanner);
    }
}
