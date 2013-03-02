package com.nearinfinity.honeycomb.hbaseclient;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class TableInfo {
    private long id;

    private String name;

    private final Map<String, byte[]> tableMetadata;

    private final Map<Long, String> idColumnNameMap;

    private final Map<String, Long> columnNameIdMap;

    private final Map<String, ColumnMetadata> metadataMap;

    public TableInfo(String name, long id) {
        this.name = name;
        this.id = id;
        this.tableMetadata = Maps.newConcurrentMap();
        this.metadataMap = Maps.newConcurrentMap();
        this.idColumnNameMap = Maps.newConcurrentMap();
        this.columnNameIdMap = Maps.newConcurrentMap();
    }

    public long getId() {
        return this.id;
    }

    public String getName() {
        return this.name;
    }

    public String getTableName() {
        int databaseIndex = this.name.indexOf(".");
        if (databaseIndex == -1) {
            throw new IllegalStateException("All tables should have a database.");
        }

        return this.name.substring(databaseIndex + 1);
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getColumnIdByName(String columnName) {
        return columnNameIdMap.get(columnName);
    }

    public String getColumnNameById(long id) {
        return idColumnNameMap.get(id);
    }

    public void addColumn(String columnName, long id, ColumnMetadata metadata) {
        idColumnNameMap.put(id, columnName);
        columnNameIdMap.put(columnName, id);
        metadataMap.put(columnName, metadata);
    }

    public Set<String> getColumnNames() {
        return this.columnNameIdMap.keySet();
    }

    public Collection<Long> getColumnIds() {
        return this.idColumnNameMap.keySet();
    }

    public Map<String, Long> columnNameToIdMap() {
        return this.columnNameIdMap;
    }

    public Map<String, Integer> columnLengthMap() {
        ImmutableMap.Builder<String, Integer> builder = ImmutableMap.builder();
        for (Map.Entry<String, ColumnMetadata> entry : this.metadataMap.entrySet()) {
            builder.put(entry.getKey(), entry.getValue().getMaxLength());
        }

        return builder.build();
    }

    public ColumnType getColumnTypeByName(String columnName) {
        ColumnMetadata info = this.metadataMap.get(columnName);

        return ColumnType.getByValue(info.getType().getValue());
    }

    public ColumnMetadata getColumnMetadata(String columnName) {
        return this.metadataMap.get(columnName);
    }

    public Map<String, byte[]> tableMetadata() {
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
        this.tableMetadata.putAll(tableInfo.tableMetadata);
        this.idColumnNameMap.putAll(tableInfo.idColumnNameMap);
        this.columnNameIdMap.putAll(tableInfo.columnNameIdMap);
        this.metadataMap.putAll(tableInfo.metadataMap);
    }

    public void setTableMetadata(Map<String, byte[]> metadata) {
        tableMetadata.clear();
        tableMetadata.putAll(metadata);
    }

    public void setColumnMetadata(String columnName, ColumnMetadata columnMetadata) {
        metadataMap.put(columnName, columnMetadata);
    }

    @Override
    public String toString() {
        return "TableInfo: " + this.write();
    }
}
