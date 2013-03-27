package com.nearinfinity.honeycomb.mysqlengine;

import com.nearinfinity.honeycomb.hbaseclient.ResultReader;
import com.nearinfinity.honeycomb.hbaseclient.Metrics;
import com.nearinfinity.honeycomb.mysql.Row;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class SingleResultScanner implements HBaseResultScanner {
    private ResultScanner scanner;
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
        if (valueToSkip == null) {
            return timedNext();
        } else { // Assumes an index scan.  Will fail otherwise
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
                rowMap = row.getRecordsLegacy();
                value = rowMap.get(this.columnName);
            } while (Arrays.equals(value, valueToSkip));

            return result;
        }
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
