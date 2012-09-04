package com.nearinfinity.hbaseclient;

import java.util.List;
import java.util.Map;
import java.util.Set;
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

    public long getColumnIdByName(String columnName) {
        return columnNamesToInfo.get(columnName).getId();
    }

    public String getColumnNameById(long id) {
        return columnIdsToInfo.get(id).getName();
    }

    public void addColumn(String columnName, long id, Map<ColumnMetadata, byte[]> metadata) {
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

        return ColumnType.getByValue(info.getMetadata().get(ColumnMetadata.COLUMN_TYPE));
    }

    public byte[] getColumnMetadata(String columnName, ColumnMetadata metadata) {
        return this.columnNamesToInfo.get(columnName).getMetadata().get(metadata);
    }
}
