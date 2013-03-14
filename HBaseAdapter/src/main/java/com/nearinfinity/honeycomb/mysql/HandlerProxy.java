package com.nearinfinity.honeycomb.mysql;

import com.nearinfinity.honeycomb.HoneycombException;
import com.nearinfinity.honeycomb.Store;
import com.nearinfinity.honeycomb.Table;
import com.nearinfinity.honeycomb.mysql.gen.TableSchema;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

public class HandlerProxy {
    private final StoreFactory storeFactory;
    private Store store;
    private Table table;
    private String tableName;

    public HandlerProxy(StoreFactory storeFactory) throws IOException, HoneycombException {
        this.storeFactory = storeFactory;
    }

    /**
     * Create a table with the given specifications.  The table is not open when
     * this is called.
     *
     * @param tableName             Name of the table
     * @param tableSpace            Indicates what store to create the table in.  If null,
     *                              create the table in the default store.
     * @param serializedTableSchema Serialized TableSchema avro object
     * @param autoInc               Initial auto increment value
     * @throws Exception
     */
    public void createTable(String tableName, String tableSpace,
                            byte[] serializedTableSchema, long autoInc) throws IOException, HoneycombException {
        Verify.isNotNullOrEmpty(tableName);
        checkNotNull(serializedTableSchema);

        this.store = this.storeFactory.createStore(tableSpace);
        TableSchema tableSchema = Util.deserializeTableSchema(serializedTableSchema);
        store.createTable(tableName, tableSchema);
        store.incrementAutoInc(tableName, autoInc);
    }

    /**
     * Drop the table with the given specifications.  The table is not open when
     * this is called.
     *
     * @param tableName     Name of the table to be dropped
     * @param tableSpace    What store to drop table from.  If null, use default.
     * @throws Exception
     */
    public void dropTable(String tableName, String tableSpace) throws IOException, HoneycombException {
        Verify.isNotNullOrEmpty(tableName);
        Store store = this.storeFactory.createStore(tableSpace);
        store.deleteTable(tableName);
    }

    public void openTable(String tableName, String tableSpace) throws IOException, HoneycombException {
        Verify.isNotNullOrEmpty(tableName);
        this.tableName = tableName;
        this.store = this.storeFactory.createStore(tableSpace);
        this.table = this.store.openTable(this.tableName);
    }

    public void closeTable() throws IOException {
        this.tableName = null;
        this.store = null;
        this.table.close();
        this.table = null;
    }

    public String getTableName() {
        return tableName;
    }

    /**
     * Updates the existing SQL table name representation in the underlying
     * {@link Store} implementation to the specified new table name.  The table
     * is not open when this is called.
     *
     * @param originalName  The existing name of the table, not null or empty
     * @param tablespace    The store which contains the table
     * @param newName       The new table name to represent, not null or empty
     * @throws Exception
     */
    public void renameTable(final String originalName, final String tableSpace,
                            final String newName) throws IOException, HoneycombException {
        Verify.isNotNullOrEmpty(originalName, "Original table name must have value.");
        Verify.isNotNullOrEmpty(newName, "New table name must have value.");
        checkArgument(!originalName.equals(newName), "New table name must be different than original.");

        Store store = this.storeFactory.createStore(tableSpace);
        store.renameTable(originalName, newName);
    }

    public long getRowCount() throws IOException, HoneycombException {
        checkTableOpen();

        return this.store.getRowCount(this.tableName);
    }

    public long getAutoIncValue()
            throws IOException, HoneycombException {
        checkTableOpen();
        if (!Verify.hasAutoIncrementColumn(store.getTableMetadata(tableName))) {
            throw new IllegalArgumentException(format("Table %s is not an autoincrement table.", this.tableName));
        }

        return store.getAutoInc(tableName);
    }

    public long incrementAutoIncrementValue(long amount) throws IOException, HoneycombException {
        checkTableOpen();
        if (!Verify.hasAutoIncrementColumn(store.getTableMetadata(tableName))) {
            throw new IllegalArgumentException(format("Column %s is not an autoincrement column.", this.tableName));
        }

        return this.store.incrementAutoInc(this.getTableName(), amount);
    }

    public void alterTable(byte[] newSchemaSerialized) throws IOException, HoneycombException {
        checkNotNull(newSchemaSerialized);
        checkTableOpen();
        TableSchema newSchema = Util.deserializeTableSchema(newSchemaSerialized);
        this.store.alterTable(this.tableName, newSchema);
    }

    public void truncateAutoIncrement() throws IOException, HoneycombException {
        checkTableOpen();
        this.store.truncateAutoInc(this.tableName);
    }

    public void incrementRowCount(int amount) throws IOException, HoneycombException {
        checkTableOpen();

        this.store.incrementRowCount(this.tableName, amount);
    }

    public void truncateRowCount() throws IOException, HoneycombException {
        checkTableOpen();
        this.store.truncateRowCount(this.tableName);
    }

    private void checkTableOpen() {
        checkState(table != null, "Table must be opened before used.");
    }
}