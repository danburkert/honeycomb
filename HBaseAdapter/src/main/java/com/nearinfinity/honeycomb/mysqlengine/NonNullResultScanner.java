package com.nearinfinity.honeycomb.mysqlengine;

import com.nearinfinity.honeycomb.hbaseclient.Metrics;
import com.nearinfinity.honeycomb.hbaseclient.ResultParser;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;
import java.util.Map;

public class NonNullResultScanner implements HBaseResultScanner {
    private ResultScanner scanner;
    private Result lastResult;
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

        Result result = timedNext();

        if (result == null) {
            return null;
        }

        Map<String, byte[]> rowMap = ResultParser.parseRowMap(result);
        byte[] value = rowMap.get(this.columnName);

        while (value == null) {
            result = timedNext();
            if (result == null) {

                return null;
            }

            rowMap = ResultParser.parseRowMap(result);
            value = rowMap.get(this.columnName);
        }

        this.lastResult = result;

        return result;
    }

    @Override
    public void close() {
        if (scanner != null) {
            this.scanner.close();
        }
    }

    @Override
    public Result getLastResult() {
        return this.lastResult;
    }

    @Override
    public void setLastResult(Result result) {
        this.lastResult = result;
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
