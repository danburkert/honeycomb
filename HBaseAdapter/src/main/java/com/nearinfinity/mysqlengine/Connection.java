package com.nearinfinity.mysqlengine;

import org.apache.hadoop.hbase.client.ResultScanner;

/**
 * Created with IntelliJ IDEA.
 * User: jedstrom
 * Date: 8/1/12
 * Time: 10:23 AM
 * To change this template use File | Settings | File Templates.
 */
public class Connection {
    private String tableName;
    private ResultScanner scanner;

    public Connection(String tableName, ResultScanner scanner) {
        this.tableName = tableName;
        this.scanner = scanner;
    }

    public ResultScanner getScanner() {
        return scanner;
    }

    public String getTableName() {
        return tableName;
    }
}
