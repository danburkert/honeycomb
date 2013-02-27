package com.nearinfinity.honeycomb;

import com.nearinfinity.honeycomb.mysql.Row;
import com.nearinfinity.honeycomb.mysql.gen.ColumnMetadata;
import com.nearinfinity.honeycomb.mysql.gen.TableMetadata;

import java.io.Closeable;
import java.io.IOException;
import java.util.UUID;

/**
 * A Table handles operations for a single MySQL table.  It must support insert,
 * update, delete and get operations on Rows, table and index scan operations,
 * and store and alter operations on the table metadata (schema).
 */
public interface Table extends Closeable {
    /**
     * Insert row into table
     * @param row Row to be inserted
     */
    public void insert(Row row);

    /**
     * Update row in table
     * @param row Row containing UUID of row to be updated, as well as updated
     *            record values.
     * @throws IOException
     * @throws RowNotFoundException
     */
    public void update(Row row) throws IOException, RowNotFoundException;

    /**
     * Remove row with given UUID from the table
     * @param uuid UUID of row to be deleted
     * @throws IOException
     * @throws RowNotFoundException
     */
    public void delete(UUID uuid) throws IOException, RowNotFoundException;

    /**
     * Flush all inserts, updates, and deletes to the table.  IUD operations are
     * not guaranteed to be visible in subsequent accesses until explicitly flushed.
     */
    public void flush() throws IOException;

    /**
     * Get row with uuid from table
     * @param uuid UUID of requested row
     * @return Row with given UUID
     */
    public Row get(UUID uuid);

    /**
     * Create a scanner for an unordered full table scan
     * @return Scanner over table
     */
    public Scanner tableScan();

    /**
     * Return a scanner over the table's index at the specified key / values in
     * ascending sort.
     * @return Scanner over index
     */
    public Scanner AscIndexScanAt(/* KeyValueContainer keyValues */);

    /**
     * Return a scanner over the table's index after the specified key / values
     * in ascending sort.
     * @return Scanner over index
     */
    public Scanner AscIndexScanAfter(/* KeyValueContainer keyValues */);

    /**
     * Return a scanner over the table's index at the specified key / values in
     * descending sort.
     * @return Scanner over index
     */
    public Scanner DescIndexScanAt(/* KeyValueContainer keyValues */);

    /**
     * Return a scanner over the table's index after the specified key / values
     * in descending sort.
     * @return Scanner over index
     */
    public Scanner DescIndexScanAfter(/* KeyValueContainer keyValues */);

    /**
     * Return a scanner over the rows in the table with the specified key /values
     * @return Scanner over index
     */
    public Scanner indexScanExact(/* KeyValueContainer keyValues */);

    /**
     * Return the current autoincrement value of the column
     * @param column The name of the column
     * @return
     * @throws IOException
     * @throws ColumnNotFoundException Thrown when the column does not exist in
     * the table, or the column is not an auto increment column
     */
    public long getAutoIncValue(String column)
            throws IOException, ColumnNotFoundException;

    /**
     * Set the autoincrement value of the column
     * @param column The name of the column
     * @param value New auto increment value
     * @return
     * @throws IOException
     * @throws ColumnNotFoundException Thrown when the column does not exist in
     * the table, or the column is not an auto increment column
     */
    public void setAutoIncValue(String column, long value)
            throws IOException, ColumnNotFoundException;

    /**
     * Return the ColumnMetadata container for the column
     * @param column Name of the column
     * @return ColumnMetadata of column
     * @throws IOException
     * @throws ColumnNotFoundException
     */
    public ColumnMetadata getColumnMetadata(String column)
            throws IOException, ColumnNotFoundException;

    /**
     * Get the table name of the Table
     * @return the table name
     * @throws IOException
     */
    public String getTableName() throws IOException;

    /**
     * Get the database name of the Table
     * @return the database name
     * @throws IOException
     */
    public String getDatabaseName() throws IOException;

    // Have not figure this one out yet
    public void alterTable(TableMetadata tableMetadata) throws IOException;
}
