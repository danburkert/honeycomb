package com.nearinfinity.honeycomb.mysql;

import com.nearinfinity.honeycomb.Store;
import com.nearinfinity.honeycomb.Table;
import com.nearinfinity.honeycomb.hbase.HBaseStore;
import com.nearinfinity.honeycomb.mysql.gen.TableSchema;

public class HandlerProxy {
    private final Store store = HBaseStore.getInstance();
    private final Table table;
    private String tableName;

    public HandlerProxy(String tableName) throws Exception {
        this.tableName = tableName;
        table = store.openTable(tableName);
    }

    public HandlerProxy(byte[] serializedTableSchema) throws Exception {
        TableSchema tableSchema = Util.deserializeTableSchema(serializedTableSchema);
        table = store.createTable(tableSchema);
        tableName = tableSchema.getName();
    }

    public Long getAutoIncValue(String columnName)
            throws Exception {
        if (!Verify.isAutoIncColumn(columnName, store.getTableMetadata(tableName))) {
            throw new IllegalArgumentException("Column " + columnName +
                    " is not an autoincrement column.");
        }

        return store.getAutoInc(tableName);
    }
}