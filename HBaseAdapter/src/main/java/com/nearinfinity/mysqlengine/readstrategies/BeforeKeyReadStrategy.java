package com.nearinfinity.mysqlengine.readstrategies;

import com.nearinfinity.mysqlengine.IndexConnection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;
import java.util.Arrays;

public class BeforeKeyReadStrategy implements IndexReadStrategy {
    private final Index client;

    public BeforeKeyReadStrategy(Index client) {
        this.client = client;
    }

    @Override
    public void setupResultScannerForConnection(IndexConnection conn, byte[] value) throws IOException {
        String tableName = conn.getTableName();
        String columnName = conn.getColumnName();
        ResultScanner indexScanner = client.getReverseIndexScanner(tableName, columnName, value);
        conn.setIndexScanner(indexScanner);

        //Get the first row of the value
        Result indexResult = conn.getNextIndexResult();
        if (indexResult == null) {
            conn.setOutOfValues();
            return;
        }

        byte[] indexValue = client.parseValueFromReverseIndexRow(tableName, columnName, indexResult);
        if (Arrays.equals(value, indexValue)) {
            //Get the next index result
            Result nextResult = conn.getNextIndexResult();
            if (nextResult == null) {
                conn.setOutOfValues();
                return ;
            }
            indexValue = client.parseValueFromReverseIndexRow(tableName, columnName, nextResult);
        }

        conn.setScanner(client.getValueIndexScanner(tableName, columnName, indexValue));
    }
}
