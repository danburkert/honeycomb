package com.nearinfinity.honeycomb;

import com.nearinfinity.honeycomb.mysql.gen.TableSchema;

import java.io.IOException;

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
     * @throws IOException
     * @throws HoneycombException
     */
    public Table openTable(String tableName) throws IOException, HoneycombException;

    /**
     * Create a table, or if the table already exists with the same name and
     * columns, open it.
     *
     * @param tableName
     * @param schema
     * @throws IOException
     */
    public void createTable(String tableName, TableSchema schema) throws IOException;

    /**
     * Delete the specified table
     *
     * @param tableName name of the table
     * @throws IOException
     * @throws HoneycombException
     */
    public void deleteTable(String tableName) throws IOException, HoneycombException;

    /**
     * Renames the specified existing table to the provided table name
     *
     * @param curTableName
     * @param newTableName
     * @throws IOException
     * @throws HoneycombException
     */
    public void renameTable(String curTableName, String newTableName) throws IOException, HoneycombException;

    /**
     * Return the table's schema
     *
     * @param tableName The table name
     * @return The table's schema
     * @throws IOException
     * @throws HoneycombException
     */
    public TableSchema getSchema(String tableName) throws IOException, HoneycombException;

    /**
     * Alter the table with the specified name.
     *
     * @param tableName The name of the table to be altered
     * @param schema    The new schema for the table
     * @throws IOException
     * @throws HoneycombException
     */
    public void alterTable(String tableName, TableSchema schema) throws IOException, HoneycombException;

    /**
     * Gets the current value of the auto increment column in the table
     *
     * @param tableName
     * @return
     * @throws IOException
     * @throws HoneycombException
     */
    public long getAutoInc(String tableName) throws IOException, HoneycombException;

    /**
     * Increment the table's auto increment value by amount
     *
     * @param tableName Name of table
     * @param amount    Amount to auto increment by
     * @return
     * @throws IOException
     * @throws HoneycombException
     */
    public long incrementAutoInc(String tableName, long amount) throws IOException, HoneycombException;

    /**
     * Truncate the table's auto increment value.
     *
     * @param tableName Name of table
     * @throws IOException
     * @throws HoneycombException
     */
    public void truncateAutoInc(String tableName) throws IOException, HoneycombException;

    /**
     * Get the table's row count
     *
     * @param tableName the table
     * @return row count
     * @throws IOException
     * @throws HoneycombException
     */
    public long getRowCount(String tableName) throws IOException, HoneycombException;

    /**
     * Increment the table's row count by amount.
     *
     * @param tableName Name of table
     * @param amount    Amount to increment by
     * @return New row count
     * @throws IOException
     * @throws HoneycombException
     */
    public long incrementRowCount(String tableName, long amount) throws IOException, HoneycombException;

    /**
     * Truncate the table's row count.
     *
     * @param tableName Name of table
     * @throws IOException
     * @throws HoneycombException
     */
    public void truncateRowCount(String tableName) throws IOException, HoneycombException;
}
