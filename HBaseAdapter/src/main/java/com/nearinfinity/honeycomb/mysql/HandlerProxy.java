package com.nearinfinity.honeycomb.mysql;

import com.nearinfinity.honeycomb.Store;
import com.nearinfinity.honeycomb.Table;
import com.nearinfinity.honeycomb.mysql.gen.TableSchema;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class HandlerProxy {
    private final StoreFactory storeFactory;
    private Store store;
    private Table table;
    private String tableName;

    public HandlerProxy(StoreFactory storeFactory) throws Exception {
        this.storeFactory = storeFactory;
    }

    public void createTable(String databaseName, String tableName, String tableSpace,
                            byte[] serializedTableSchema, long autoInc) throws Exception {
        checkTableName(tableName);
        this.store = this.storeFactory.createStore(databaseName);
        TableSchema tableSchema = Util.deserializeTableSchema(serializedTableSchema);
        store.createTable(tableName, tableSchema);
        if (autoInc > 0) {
            store.incrementAutoInc(tableName, autoInc);
        }
        this.store = null;
    }

    public void openTable(String databaseName, String tableName) throws Exception {
        checkTableName(tableName);
        this.store = this.storeFactory.createStore(databaseName);
        this.tableName = tableName;
        this.table = this.store.openTable(this.tableName);
    }

    public String getTableName() {
        return tableName;
    }

    /**
     * Updates the existing SQL table name representation in the underlying
     * {@link Store} implementation to the specified new table name
     *
     * @param newName The new table name to represent, not null or empty
     * @throws Exception
     */
    public void renameTable(final String newName) throws Exception {
        checkNotNull(newName, "The specified table name is invalid");
        checkArgument(!newName.isEmpty(), "The specified table name cannot be empty");

        store.renameTable(tableName, newName);
        tableName = newName;
    }

    public long getAutoIncValue(String columnName)
            throws Exception {
        if (!Verify.isAutoIncColumn(columnName, store.getTableMetadata(tableName))) {
            throw new IllegalArgumentException("Column " + columnName +
                    " is not an autoincrement column.");
        }

        return store.getAutoInc(tableName);
    }

    public void dropTable() throws Exception {
        this.store.deleteTable(this.tableName);
    }

    private void checkTableName(String tableName) {
        checkNotNull(tableName);
        checkArgument(!tableName.isEmpty());
    }
}