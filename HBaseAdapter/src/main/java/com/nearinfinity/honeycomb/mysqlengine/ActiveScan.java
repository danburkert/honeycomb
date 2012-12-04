package com.nearinfinity.honeycomb.mysqlengine;

import java.util.Arrays;
import java.util.List;

public class ActiveScan {
    private String tableName;
    private String columnName;
    private HBaseResultScanner scanner;

    public ActiveScan(String tableName, HBaseResultScanner scanner) {
        this(tableName, null, scanner);
    }

    public ActiveScan(String tableName, String columnName) {
        this(tableName, columnName, null);
    }

    public ActiveScan(String tableName, String columnName, HBaseResultScanner scanner) {
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

    public List<String> getColumnName() {
        return Arrays.asList(this.columnName.split(","));
    }

}
