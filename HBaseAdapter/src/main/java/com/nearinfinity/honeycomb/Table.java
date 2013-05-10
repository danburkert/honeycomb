package com.nearinfinity.honeycomb;

import com.nearinfinity.honeycomb.mysql.QueryKey;
import com.nearinfinity.honeycomb.mysql.Row;
import com.nearinfinity.honeycomb.mysql.schema.IndexSchema;

import java.io.Closeable;
import java.util.Collection;
import java.util.UUID;

/**
 * A Table handles operations for a single MySQL table. It must support "insert",
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
     * @param indexSchema The {@link com.nearinfinity.honeycomb.mysql.schema.IndexSchema} representing the index details, not null
     */
    void insertTableIndex(final IndexSchema indexSchema);

    /**
     * Update row in table
     *
     *
     * @param oldRow         The old row
     * @param newRow         The new row
     * @param changedIndices List of indices with updated values.
     * @throws com.nearinfinity.honeycomb.exceptions.RowNotFoundException
     *
     */
    void update(Row oldRow, Row newRow, Collection<IndexSchema> changedIndices);

    /**
     * Remove row from the table
     *
     * @param row The row to be deleted
     * @throws com.nearinfinity.honeycomb.exceptions.RowNotFoundException
     *
     */
    void delete(Row row);

    /**
     * Deletes the index corresponding to the specified index name from the table
     *
     * @param indexSchema The {@link com.nearinfinity.honeycomb.mysql.schema.IndexSchema} representing the index details, not null
     */
    void deleteTableIndex(final IndexSchema indexSchema);

    /**
     * Flush all inserts, updates, and deletes to the table. IUD operations are
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
    Scanner ascendingIndexScanAt(QueryKey key);

    /**
     * Return a scanner over the table's index after the specified key / values
     * in ascending sort.
     *
     * @param key
     * @return Scanner over index
     */
    Scanner ascendingIndexScanAfter(QueryKey key);

    /**
     * Return a scanner over the table's index at the specified key / values in
     * descending sort.
     *
     * @param key
     * @return Scanner over index
     */
    Scanner descendingIndexScanAt(QueryKey key);

    /**
     * Return a scanner over the table's index after the specified key / values
     * in descending sort.
     *
     * @param key
     * @return Scanner over index
     */
    Scanner descendingIndexScanAfter(QueryKey key);

    /**
     * Return a scanner over the rows in the table with the specified key /values
     *
     * @param key
     * @return Scanner over index
     */
    Scanner indexScanExact(QueryKey key);

    /**
     * Remove all rows from the table.
     */
    void deleteAllRows();
}