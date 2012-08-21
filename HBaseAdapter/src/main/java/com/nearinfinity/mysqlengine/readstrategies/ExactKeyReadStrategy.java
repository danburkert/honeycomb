package com.nearinfinity.mysqlengine.readstrategies;

import com.nearinfinity.mysqlengine.HBaseClient;
import com.nearinfinity.mysqlengine.IndexConnection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;

public class ExactKeyReadStrategy implements IndexReadStrategy {

    private final Index client;

    public ExactKeyReadStrategy(Index client) {
        this.client = client;
    }

    @Override
    public void setupResultScannerForConnection(IndexConnection conn, byte[] value) throws IOException {
        String tableName = conn.getTableName();
        String columnName = conn.getColumnName();
        ResultScanner indexScanner = client.getSecondaryIndexScannerExact(tableName, columnName, value);
        conn.setIndexScanner(indexScanner);

        //Get the first row of the value
        Result indexResult = conn.getNextIndexResult();
        if (indexResult == null) {
            conn.setOutOfValues();
            return;
        }

        conn.setScanner(client.getValueIndexScanner(tableName, columnName, value));
    }
}
