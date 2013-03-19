package com.nearinfinity.honeycomb;

import com.nearinfinity.honeycomb.mysql.gen.TableSchema;

/**
 * The store is responsible for meta operations on tables: opening, creating,
 * altering, deleting, and retrieving metadata.
 */
public interface Store {

    /**
     * Return the table
     *
     * @param tableName The name of the table
     * @return the table
     */
    public Table openTable(String tableName);

    /**
     * Create a table, or if the table already exists with the same name and
     * columns, open it.
     *
     * @param tableName
     * @param schema
     * @
     */
    public void createTable(String tableName, TableSchema schema);

    /**
     * Delete the specified table
     *
     * @param tableName name of the table
     */
    public void deleteTable(String tableName);

    /**
     * Renames the specified existing table to the provided table name
     *
     * @param curTableName
     * @param newTableName
     */
    public void renameTable(String curTableName, String newTableName);

    /**
     * Return the table's schema
     *
     * @param tableName The table name
     * @return The table's schema
     */
    public TableSchema getSchema(String tableName);

    /**
     * Alter the table with the specified name.
     *
     * @param tableName The name of the table to be altered
     * @param schema    The new schema for the table
     */
    public void alterTable(String tableName, TableSchema schema);

    /**
     * Gets the current value of the auto increment column in the table
     *
     * @param tableName
     * @return
     * @throws HoneycombException
     * @
     */
    public long getAutoInc(String tableName);

    /**
     * Increment the table's auto increment value by amount
     *
     * @param tableName Name of table
     * @param amount    Amount to auto increment by
     * @return
     * @throws HoneycombException
     * @
     */
    public long incrementAutoInc(String tableName, long amount);

    /**
     * Truncate the table's auto increment value.
     *
     * @param tableName Name of table
     */
    public void truncateAutoInc(String tableName);

    /**
     * Get the table's row count
     *
     * @param tableName the table
     * @return row count
     */
    public long getRowCount(String tableName);

    /**
     * Increment the table's row count by amount.
     *
     * @param tableName Name of table
     * @param amount    Amount to increment by
     * @return New row count
     */
    public long incrementRowCount(String tableName, long amount);

    /**
     * Truncate the table's row count.
     *
     * @param tableName Name of table
     */
    public void truncateRowCount(String tableName);
}
