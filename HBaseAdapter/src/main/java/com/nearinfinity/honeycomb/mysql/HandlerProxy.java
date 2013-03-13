package com.nearinfinity.honeycomb.mysql;

import com.nearinfinity.honeycomb.Store;
import com.nearinfinity.honeycomb.Table;
import com.nearinfinity.honeycomb.mysql.gen.TableSchema;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

public class HandlerProxy {
    private final StoreFactory storeFactory;
    private Store store;
    private Table table;
    private String tableName;
    private boolean isTableOpen;

    public HandlerProxy(StoreFactory storeFactory) throws Exception {
        this.storeFactory = storeFactory;
    }

    /**
     * Create a table with the given specifications
     *
     * @param databaseName          Database containing the table
     * @param tableName             Name of the table
     * @param tableSpace            Indicates what store to create the table in.  If null,
     *                              create the table in the default store.
     * @param serializedTableSchema Serialized TableSchema avro object
     * @param autoInc               Initial auto increment value
     * @throws Exception
     */
    public void createTable(String databaseName, String tableName, String tableSpace,
                            byte[] serializedTableSchema, long autoInc) throws Exception {
        Verify.isNotNullOrEmpty(tableName);
        Verify.isNotNullOrEmpty(databaseName);
        checkNotNull(serializedTableSchema);

        this.store = this.storeFactory.createStore(tableSpace);
        TableSchema tableSchema = Util.deserializeTableSchema(serializedTableSchema);
        store.createTable(Util.fullyQualifyTable(databaseName, tableName), tableSchema);
        if (autoInc > 0) {
            store.incrementAutoInc(tableName, autoInc);
        }
    }

    public void openTable(String databaseName, String tableName, String tableSpace) throws Exception {
        Verify.isNotNullOrEmpty(tableName);
        Verify.isNotNullOrEmpty(databaseName);
        this.store = this.storeFactory.createStore(tableSpace);
        this.tableName = Util.fullyQualifyTable(databaseName, tableName);
        this.table = this.store.openTable(this.tableName);
        this.isTableOpen = true;
    }

    public String getTableName() {
        return tableName;
    }

    /**
     * Updates the existing SQL table name representation in the underlying
     * {@link Store} implementation to the specified new table name
     *
     * @param databaseName Database of the old and new table
     * @param newName      The new table name to represent, not null or empty
     * @throws Exception
     */
    public void renameTable(String databaseName, final String newName) throws Exception {
        Verify.isNotNullOrEmpty(newName, "New table name must have value.");
        checkTableOpen();

        String newTableName = Util.fullyQualifyTable(databaseName, newName);
        store.renameTable(tableName, newTableName);
        tableName = newTableName;
    }

    public long getAutoIncValue(String columnName)
            throws Exception {
        checkTableOpen();
        if (!Verify.isAutoIncColumn(columnName, store.getTableMetadata(tableName))) {
            throw new IllegalArgumentException("Column " + columnName +
                    " is not an autoincrement column.");
        }

        return store.getAutoInc(tableName);
    }

    public void dropTable() throws Exception {
        checkTableOpen();
        this.store.deleteTable(this.tableName);
        this.isTableOpen = false;
    }

    public void alterTable(byte[] newSchemaSerialized) throws Exception {
        checkNotNull(newSchemaSerialized);
        checkTableOpen();
        TableSchema newSchema = Util.deserializeTableSchema(newSchemaSerialized);
        this.store.alterTable(this.tableName, newSchema);
    }

    private void checkTableOpen() {
        checkState(isTableOpen, "Table must be opened before used.");
    }
}