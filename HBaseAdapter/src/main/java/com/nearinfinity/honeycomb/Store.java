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
     * @param name The name of the table
     * @return the table
     * @throws TableNotFoundException
     */
    public Table openTable(String name) throws TableNotFoundException;

    /**
     * Return the table's metadata
     *
     * @param name The table name
     * @return The table's metadata
     * @throws TableNotFoundException
     */
    public TableSchema getTableMetadata(String name) throws TableNotFoundException;

    /**
     * Create a table, or if the table already exists with the same name and
     * columns, open it.
     *
     * @param schema
     * @return
     * @throws IOException
     */
    public Table createTable(TableSchema schema) throws IOException /*TableExistsException?*/;

    /**
     * Delete the specified table
     *
     * @param name name of the table
     * @throws IOException
     */
    public void deleteTable(String name) throws IOException;
}
