package com.nearinfinity.honeycomb;

import com.nearinfinity.honeycomb.mysql.gen.TableMetadata;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * The store is a singleton which opens, creates, and deletes Tables.
 */
public interface Store {
    /**
     * Get the singleton instance of the Store
     *
     * @return the store
     */
    public Store getStore(Configuration configuration) throws IOException;

    /**
     * Return the table
     *
     * @param name The name of the table
     * @return the table
     * @throws TableNotFoundException
     */
    public Table open(String name) throws TableNotFoundException;

    /**
     * Create a table, or if the table already exists with the same name and
     * columns, open it.
     *
     * @param metadata
     * @return
     * @throws IOException
     */
    public Table create(TableMetadata metadata) throws IOException /*TableExistsException?*/;

    /**
     * Delete the specified table
     *
     * @param name name of the table
     * @throws IOException
     */
    public void deleteTable(String name) throws IOException;
}
