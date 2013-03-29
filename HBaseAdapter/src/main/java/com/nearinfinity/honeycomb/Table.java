package com.nearinfinity.honeycomb;

import java.io.Closeable;
import java.util.UUID;

import com.nearinfinity.honeycomb.mysql.IndexKey;
import com.nearinfinity.honeycomb.mysql.Row;
import com.nearinfinity.honeycomb.mysql.gen.IndexSchema;

/**
 * A Table handles operations for a single MySQL table.  It must support "insertRow",
 * "update", "delete" and "get" operations on rows, as well as table and index scans
 */
public interface Table extends Closeable {
    /**
     * Insert row into table
     *
     * @param row Row to be inserted
     */
    void insert(Row row);

    /**
     * Inserts an index on the table
     *
     * @param indexName The identifying name of the index, not null or empty
     * @param indexSchema The {@link IndexSchema} representing the index details, not null
     */
    void insertTableIndex(final String indexName, final IndexSchema indexSchema);

    /**
     * Update row in table
     *
     * @param row Row containing UUID of row to be updated, as well as updated
     *            record values.
     * @throws com.nearinfinity.honeycomb.exceptions.RowNotFoundException
     */
    void update(Row row);

    /**
     * Remove row with given UUID from the table
     *
     * @param uuid UUID of row to be deleted
     * @throws com.nearinfinity.honeycomb.exceptions.RowNotFoundException
     */
    void delete(UUID uuid);

    /**
     * Deletes the index corresponding to the specified index name from the table
     *
     * @param indexName The identifying name of the index, not null or empty
     * @param indexSchema The {@link IndexSchema} representing the index details, not null
     */
    void deleteTableIndex(final String indexName, final IndexSchema indexSchema);

    /**
     * Flush all inserts, updates, and deletes to the table.  IUD operations are
     * not guaranteed to be visible in subsequent accesses until explicitly flushed.
     */
    void flush();

    /**
     * Get row with uuid from table
     *
     * @param uuid UUID of requested row
     * @return Row with given UUID
     */
    Row get(UUID uuid);

    /**
     * Create a scanner for an unordered full table scan
     *
     * @return Scanner over table
     */
    Scanner tableScan();

    /**
     * Return a scanner over the table's index at the specified key / values in
     * ascending sort.
     *
     * @param key
     * @return Scanner over index
     */
    Scanner ascendingIndexScanAt(IndexKey key);

    /**
     * Return a scanner over the table's index after the specified key / values
     * in ascending sort.
     *
     * @param key
     * @return Scanner over index
     */
    Scanner ascendingIndexScanAfter(IndexKey key);

    /**
     * Return a scanner over the table's index at the specified key / values in
     * descending sort.
     *
     * @param key
     * @return Scanner over index
     */
    Scanner descendingIndexScanAt(IndexKey key);

    /**
     * Return a scanner over the table's index after the specified key / values
     * in descending sort.
     *
     * @param key
     * @return Scanner over index
     */
    Scanner descendingIndexScanAfter(IndexKey key);

    /**
     * Return a scanner over the rows in the table with the specified key /values
     *
     * @param key
     * @return Scanner over index
     */
    Scanner indexScanExact(IndexKey key);

    /**
     * Remove all rows from the table.
     */
    void deleteAllRows();
}