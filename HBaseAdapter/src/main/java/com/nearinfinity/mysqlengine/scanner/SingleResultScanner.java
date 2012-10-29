package com.nearinfinity.mysqlengine.scanner;

import com.nearinfinity.hbaseclient.ResultParser;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

public class SingleResultScanner implements HBaseResultScanner {
    private ResultScanner scanner;
    private Result lastResult;
    private String columnName;

    public SingleResultScanner(ResultScanner scanner) {
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

        if (valueToSkip != null) {
            Map<String, byte[]> rowMap = ResultParser.parseRowMap(result);
            byte[] value = rowMap.get(this.columnName);

            while (Arrays.equals(valueToSkip, value)) {
                result = this.scanner.next();
                rowMap = ResultParser.parseRowMap(result);
                value = rowMap.get(this.columnName);
            }
        }

        byte[] value = null;

        while (valueToSkip != null && Arrays.equals(value, valueToSkip))
        {
            result = this.scanner.next();
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
}
