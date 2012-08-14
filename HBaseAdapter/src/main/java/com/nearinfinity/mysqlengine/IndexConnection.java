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
    private ResultScanner scanner;
    private int currentIndex;
    private String columnName;

    public IndexConnection(String tableName, String columnName) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.currentIndex = 0;
        this.scanner = null;
    }

    public String getTableName() {
        return this.tableName;
    }

    public Result getLastResult() {
        return null;
    }

    public Result getNextResult() throws IOException {
        return this.scanner.next();
    }

    public void close() {
        this.scanner.close();
    }

    public String getColumnName() {
        return this.columnName;
    }

    public void setScanner(ResultScanner scanner) {
        this.scanner = scanner;
    }
}
