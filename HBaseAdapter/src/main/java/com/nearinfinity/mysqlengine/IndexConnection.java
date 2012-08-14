package com.nearinfinity.mysqlengine;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: jedstrom
 * Date: 8/14/12
 * Time: 11:08 AM
 * To change this template use File | Settings | File Templates.
 */
public class IndexConnection implements Connection {
    private String tableName;
    private Result[] results;
    private int currentIndex;
    private String columnName;

    public IndexConnection(String tableName, String columnName) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.currentIndex = 0;
        this.results = null;
    }

    public String getTableName() {
        return this.tableName;
    }

    public Result getLastResult() {
        if (currentIndex <= 0) {
            return null;
        }

        return results[currentIndex-1];
    }

    public Result getNextResult() throws IOException {
        if (currentIndex >= results.length) {
            return null;
        }

        return results[currentIndex++];
    }

    public void close() {
        results = null;
    }

    public String getColumnName() {
        return this.columnName;
    }
}
