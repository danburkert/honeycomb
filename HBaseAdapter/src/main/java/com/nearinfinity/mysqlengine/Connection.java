package com.nearinfinity.mysqlengine;

import com.nearinfinity.hbaseclient.HBaseClient;
import com.nearinfinity.mysqlengine.jni.IndexReadType;
import com.nearinfinity.mysqlengine.scanner.HBaseResultScanner;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: jedstrom
 * Date: 8/1/12
 * Time: 10:23 AM
 * To change this template use File | Settings | File Templates.
 */
public class Connection {
    private String tableName;
    private String columnName;
    private HBaseResultScanner scanner;

    public Connection(String tableName, HBaseResultScanner scanner) {
        this(tableName, null, scanner);
    }

    public Connection(String tableName, String columnName) {
        this(tableName, columnName, null);
    }

    public Connection(String tableName, String columnName, HBaseResultScanner scanner) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.scanner = scanner;
    }

    public void setScanner(HBaseResultScanner scanner) {
        this.scanner = scanner;
    }

    public HBaseResultScanner getScanner() {
        return this.scanner;
    }

    public void close() {
        if (scanner == null) {
            scanner.close();
        }
    }

    public String getTableName() {
        return this.tableName;
    }

    public String getColumnName() {
        return this.columnName;
    }

}
