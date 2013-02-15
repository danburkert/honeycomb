package com.nearinfinity.honeycomb.mysqlengine;

import com.nearinfinity.honeycomb.hbaseclient.Metrics;
import com.nearinfinity.honeycomb.hbaseclient.ResultParser;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class SingleResultScanner implements HBaseResultScanner {
    private ResultScanner scanner;
    private Result lastResult;
    private String columnName;

    public SingleResultScanner(ResultScanner scanner) {
        this.scanner = scanner;
    }

    /**
     * Retrieve the next value in an HBase scan if passed null.
     * Otherwise, retrieve the next value in an HBase scan that is not the value passed in.
     *
     * @param valueToSkip A value to skip over while looking for the next result
     * @return HBase result from the scan
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

        if (valueToSkip != null) {
            Map<String, byte[]> rowMap = ResultParser.parseRowMap(result);
            byte[] value = rowMap.get(this.columnName);

            while (Arrays.equals(valueToSkip, value)) {
                result = timedNext();
                if (result == null) {
                    return null;
                }

                rowMap = ResultParser.parseRowMap(result);
                value = rowMap.get(this.columnName);
            }
        }

        byte[] value = null;

        while (valueToSkip != null && Arrays.equals(value, valueToSkip)) {
            result = timedNext();
            value = ResultParser.parseValueMap(result);
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
