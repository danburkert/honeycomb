package com.nearinfinity.hbaseclient;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TableInfo {
    private long id;

    private String name;

    private ConcurrentHashMap<String, ColumnInfo> columnNamesToInfo;

    private ConcurrentHashMap<Long, ColumnInfo> columnIdsToInfo;

    public TableInfo(String name, long id) {
        this.name = name;
        this.id = id;
        this.columnNamesToInfo = new ConcurrentHashMap<String, ColumnInfo>();
        this.columnIdsToInfo = new ConcurrentHashMap<Long, ColumnInfo>();
    }

    public long getId() {
        return this.id;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getColumnIdByName(String columnName) {
        return columnNamesToInfo.get(columnName).getId();
    }

    public String getColumnNameById(long id) {
        return columnIdsToInfo.get(id).getName();
    }

    public void addColumn(String columnName, long id, ColumnMetadata metadata) {
        ColumnInfo info = new ColumnInfo(id, columnName, metadata);
        columnNamesToInfo.put(columnName, info);
        columnIdsToInfo.put(id, info);
    }

    public Set<String> getColumnNames() {
        return this.columnNamesToInfo.keySet();
    }

    public Set<Long> getColumnIds() {
        return this.columnIdsToInfo.keySet();
    }

    public ColumnType getColumnTypeByName(String columnName) {
        ColumnInfo info = this.columnNamesToInfo.get(columnName);

        return ColumnType.getByValue(info.getMetadata().getType().getValue());
    }

    public ColumnMetadata getColumnMetadata(String columnName) {
        return this.columnNamesToInfo.get(columnName).getMetadata();
    }
}
