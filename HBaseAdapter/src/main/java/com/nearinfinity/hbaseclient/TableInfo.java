package com.nearinfinity.hbaseclient;

import com.google.gson.Gson;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TableInfo {
    private long id;

    private String name;

    private final ConcurrentHashMap<String, ColumnInfo> columnNamesToInfo;

    private final ConcurrentHashMap<Long, ColumnInfo> columnIdsToInfo;

    private final ConcurrentHashMap<String, Long> columnNameToId;

    private final ConcurrentHashMap<byte[], byte[]> tableMetadata;

    public TableInfo(String name, long id) {
        this.name = name;
        this.id = id;
        this.columnNamesToInfo = new ConcurrentHashMap<String, ColumnInfo>();
        this.columnIdsToInfo = new ConcurrentHashMap<Long, ColumnInfo>();
        this.columnNameToId = new ConcurrentHashMap<String, Long>();
        this.tableMetadata = new ConcurrentHashMap<byte[], byte[]>();
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
        columnNameToId.put(columnName, id);
    }

    public Set<String> getColumnNames() {
        return this.columnNamesToInfo.keySet();
    }

    public Set<Long> getColumnIds() {
        return this.columnIdsToInfo.keySet();
    }

    public Map<String, Long> columnNameToIdMap() {
        return this.columnNameToId;
    }

    public ColumnType getColumnTypeByName(String columnName) {
        ColumnInfo info = this.columnNamesToInfo.get(columnName);

        return ColumnType.getByValue(info.getMetadata().getType().getValue());
    }

    public ColumnMetadata getColumnMetadata(String columnName) {
        return this.columnNamesToInfo.get(columnName).getMetadata();
    }

    public Map<byte[], byte[]> tableMetadata() {
        return this.tableMetadata;
    }

    public String write() {
        return new Gson().toJson(this);
    }

    public void read(String data) {
        this.copy(new Gson().fromJson(data, TableInfo.class));
    }

    private void copy(TableInfo tableInfo) {
        this.id = tableInfo.id;
        this.name = tableInfo.name;
        this.columnIdsToInfo.putAll(tableInfo.columnIdsToInfo);
        this.columnNamesToInfo.putAll(tableInfo.columnNamesToInfo);
        this.columnNameToId.putAll(tableInfo.columnNameToId);
        this.tableMetadata.putAll(tableInfo.tableMetadata);
    }

    public void setTableMetadata(Map<byte[], byte[]> metadata) {
        tableMetadata.clear();
        tableMetadata.putAll(metadata);
    }
}
