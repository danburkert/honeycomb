package com.nearinfinity.mysqlengine;

import java.util.List;
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

    public void addColumn(String columnName, long id, List<ColumnMetadata> metadata) {
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

    public ColumnMetadata getColumnTypeByName(String columnName) {
        ColumnInfo info = this.columnNamesToInfo.get(columnName);
        for (ColumnMetadata metadata : info.getMetadata()) {
            switch (metadata) {
                case LONG:
                case DOUBLE:
                case STRING:
                case TIME:
                    return metadata;
                default:
                    // Keep going
            }
        }

        return ColumnMetadata.NONE;
    }
}
