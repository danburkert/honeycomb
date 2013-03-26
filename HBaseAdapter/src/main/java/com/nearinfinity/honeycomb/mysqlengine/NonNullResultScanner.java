package com.nearinfinity.honeycomb.mysqlengine;

import com.nearinfinity.honeycomb.hbaseclient.ResultReader;
import com.nearinfinity.honeycomb.hbaseclient.Metrics;
import com.nearinfinity.honeycomb.mysql.Row;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;
import java.util.Map;

public class NonNullResultScanner implements HBaseResultScanner {
    private ResultScanner scanner;
    private String columnName;

    public NonNullResultScanner(ResultScanner scanner) {
        this.scanner = scanner;
    }

    /**
     * Retrieve the next value from HBase where the index column is not null in the map.
     *
     * @param valueToSkip Ignored
     * @return Next non-null result
     * @throws IOException
     */
    @Override
    public Result next(byte[] valueToSkip) throws IOException {
        if (scanner == null) {
            return null;
        }
        Result result;
        Row row;
        Map<String, byte[]> rowMap;
        byte[] value;

        do {
            result = timedNext();
            if (result == null) {
                return null;
            }
            row = ResultReader.readIndexRow(result);
            rowMap = row.getRecords();
            value = rowMap.get(this.columnName);


        } while (value == null);

        return result;
    }

    @Override
    public void close() {
        if (scanner != null) {
            this.scanner.close();
        }
    }

    @Override
    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    private Result timedNext() throws IOException {
        long start = System.currentTimeMillis();
        Result result = this.scanner.next();
        long end = System.currentTimeMillis();
        Metrics.getInstance().addHBaseTime(end - start);
        return result;
    }
}
