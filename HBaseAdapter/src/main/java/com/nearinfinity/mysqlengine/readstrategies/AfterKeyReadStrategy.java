package com.nearinfinity.mysqlengine.readstrategies;

import com.nearinfinity.mysqlengine.IndexConnection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;
import java.util.Arrays;

public class AfterKeyReadStrategy implements IndexReadStrategy {
    private final Index client;

    public AfterKeyReadStrategy(Index client) {
        this.client = client;
    }

    @Override
    public void setupResultScannerForConnection(IndexConnection conn, byte[] value) throws IOException {
        String tableName = conn.getTableName();
        String columnName = conn.getColumnName();
        ResultScanner indexScanner = client.getSecondaryIndexScanner(tableName, columnName, value);
        conn.setIndexScanner(indexScanner);

        //Get the first row of the value
        Result indexResult = conn.getNextIndexResult();
        if (indexResult == null) {
            conn.setOutOfValues();
            return;
        }

        byte[] nextValue = client.parseValueFromSecondaryIndexRow(tableName, columnName, indexResult);
        if (Arrays.equals(value, nextValue)) {
            //Get the next index result
            Result nextResult = conn.getNextIndexResult();
            if (nextResult == null) {
                conn.setOutOfValues();
                return;
            }
            nextValue = client.parseValueFromSecondaryIndexRow(tableName, columnName, nextResult);
        }

        conn.setScanner(client.getValueIndexScanner(tableName, columnName, nextValue));
    }
}
