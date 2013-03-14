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
     * @throws TableNotFoundException
     */
    public Table openTable(String tableName) throws Exception;

    /**
     * Create a table, or if the table already exists with the same name and
     * columns, open it.
     *
     * @param tableName
     * @param schema
     * @throws IOException
     */
    public void createTable(String tableName, TableSchema schema) throws Exception;

    /**
     * Delete the specified table
     *
     * @param tableName name of the table
     * @throws IOException
     */
    public void deleteTable(String tableName) throws Exception;

    /**
     * Renames the specified existing table to the provided table name
     *
     * @param curTableName
     * @param newTableName
     * @throws Exception
     */
    public void renameTable(String curTableName, String newTableName) throws Exception;

    /**
     * Return the table's metadata
     *
     * @param tableName The table name
     * @return The table's metadata
     * @throws TableNotFoundException
     */
    public TableSchema getSchema(String tableName) throws Exception;

    /**
     * Alter the table with the specified name.
     *
     * @param tableName The name of the table to be altered
     * @param schema    The new schema for the table
     * @throws TableNotFoundException
     * @throws IOException
     */
    public void alterTable(String tableName, TableSchema schema) throws Exception;

    /**
     * Gets the current value of the auto increment column in the table
     *
     * @param tableName
     * @return
     * @throws Exception
     */
    public long getAutoInc(String tableName) throws Exception;

    /**
     * Increment the table's auto increment value by amount
     *
     * @param tableName Name of table
     * @param amount    Amount to auto increment by
     * @return
     * @throws Exception
     */
    public long incrementAutoInc(String tableName, long amount) throws Exception;

    /**
     * Truncate the table's auto increment value.
     *
     * @param tableName Name of table
     * @throws Exception
     */
    public void truncateAutoInc(String tableName) throws Exception;

    /**
     * Get the table's row count
     *
     * @param tableName the table
     * @return row count
     * @throws Exception
     */
    public long getRowCount(String tableName) throws Exception;

    /**
     * Increment the table's row count by amount.
     *
     * @param tableName Name of table
     * @param amount    Amount to increment by
     * @return New row count
     */
    public long incrementRowCount(String tableName, long amount) throws Exception;

    /**
     * Truncate the table's row count.
     * @param tableName Name of table
     * @throws Exception
     */
    public void truncateRowCount(String tableName) throws Exception;
}