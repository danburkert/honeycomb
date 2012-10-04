package com.nearinfinity.hbaseclient;

import com.google.gson.Gson;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TableInfo implements Writable {
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

    @Override
    public void write(DataOutput out) throws IOException {
        out.write(new Gson().toJson(this).getBytes());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.copy(new Gson().fromJson(new String(Bytes.readByteArray(in)), TableInfo.class));
    }

    private void copy(TableInfo tableInfo) {
        this.id = tableInfo.id;
        this.name = tableInfo.name;
        this.columnIdsToInfo.putAll(tableInfo.columnIdsToInfo);
        this.columnNamesToInfo.putAll(tableInfo.columnNamesToInfo);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TableInfo tableInfo = (TableInfo) o;

        if (id != tableInfo.id) return false;
        if (columnIdsToInfo != null ? !columnIdsToInfo.equals(tableInfo.columnIdsToInfo) : tableInfo.columnIdsToInfo != null)
            return false;
        if (columnNamesToInfo != null ? !columnNamesToInfo.equals(tableInfo.columnNamesToInfo) : tableInfo.columnNamesToInfo != null)
            return false;
        if (name != null ? !name.equals(tableInfo.name) : tableInfo.name != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (id ^ (id >>> 32));
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (columnNamesToInfo != null ? columnNamesToInfo.hashCode() : 0);
        result = 31 * result + (columnIdsToInfo != null ? columnIdsToInfo.hashCode() : 0);
        return result;
    }
}
