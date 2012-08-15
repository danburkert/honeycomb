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
public class DataConnection implements Connection {
    private String tableName;
    private ResultScanner scanner;
    private Result lastResult;

    public DataConnection(String tableName, ResultScanner scanner) {
        this.tableName = tableName;
        this.scanner = scanner;
        this.lastResult = null;
    }

    public String getTableName() {
        return this.tableName;
    }

    public Result getLastResult() {
        return this.lastResult;
    }

    public Result getNextResult() throws IOException {
        Result result = this.scanner.next();
        this.lastResult = result;
        return result;
    }

    public void close() {
        this.scanner.close();
    }

    public void setScanner(ResultScanner scanner) {
        this.scanner = scanner;
        this.lastResult = null;
    }
}
