package com.nearinfinity.honeycomb.mysqlengine;

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

    @Override
    public Result next(byte[] valueToSkip) throws IOException {
        if (scanner == null) {
            return null;
        }

        Result result = this.scanner.next();

        if (result == null) {
            return null;
        }

        Map<String, byte[]> rowMap = ResultParser.parseRowMap(result);
        byte[] value = rowMap.get(this.columnName);

        while (value == null) {
            result = this.scanner.next();
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
}
