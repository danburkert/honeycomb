package com.nearinfinity.mysqlengine;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created with IntelliJ IDEA.
 * User: jedstrom
 * Date: 7/25/12
 * Time: 2:08 PM
 * To change this template use File | Settings | File Templates.
 */
public class TableInfo {
    private long id;

    private String name;

    private ConcurrentHashMap<String, ColumnInfo> columnNamesToIds;

    private ConcurrentHashMap<Long, ColumnInfo> columnIdsToNames;

    public TableInfo(String name, long id) {
        this.name = name;
        this.id = id;
        this.columnNamesToIds = new ConcurrentHashMap<String, ColumnInfo>();
        this.columnIdsToNames = new ConcurrentHashMap<Long, ColumnInfo>();
    }

    public long getId() {
        return this.id;
    }

    public String getName() {
        return this.name;
    }

    public long getColumnIdByName(String columnName) {
        return columnNamesToIds.get(columnName).getId();
    }

    public String getColumnNameById(long id) {
        return columnIdsToNames.get(id).getName();
    }

    public void addColumn(String columnName, long id) {
        ColumnInfo info = new ColumnInfo(id, columnName);
        columnNamesToIds.put(columnName, info);
        columnIdsToNames.put(id, info);
    }
}
