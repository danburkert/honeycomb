package com.nearinfinity.mysqlengine;

import com.nearinfinity.mysqlengine.scanner.HBaseResultScanner;

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
        if (scanner != null) {
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
