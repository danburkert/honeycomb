package com.nearinfinity.honeycomb.mysql;

import com.google.common.base.Preconditions;
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

    public HandlerProxy(String tableName, byte[] serializedTableSchema) throws Exception {
        TableSchema tableSchema = Util.deserializeTableSchema(serializedTableSchema);
        table = store.createTable(tableName, tableSchema);
        this.tableName = tableName;
    }

    /**
     * Updates the existing SQL table name representation in the underlying
     * {@link Store} implementation to the specified new table name
     * @param newName The new table name to represent, not null or empty
     * @throws Exception
     */
    public void renameTable(final String newName) throws Exception {
        Preconditions.checkNotNull(newName, "The specified table name is invalid");
        Preconditions.checkArgument(!newName.isEmpty(), "The specified table name cannot be empty");

        store.renameTable(tableName, newName);
        tableName = newName;
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