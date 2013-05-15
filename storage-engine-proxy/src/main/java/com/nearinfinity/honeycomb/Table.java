/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 * Copyright 2013 Altamira Corporation.
 */


package com.nearinfinity.honeycomb;

import java.io.Closeable;
import java.util.Collection;
import java.util.UUID;

import com.nearinfinity.honeycomb.mysql.QueryKey;
import com.nearinfinity.honeycomb.mysql.Row;
import com.nearinfinity.honeycomb.mysql.schema.IndexSchema;

/**
 * A Table handles operations for a single MySQL table. It must support "insert",
 * "updateRow", "deleteRow" and "getRow" operations on rows, table and index scans,
 * and building and removing indices.
 */
public interface Table extends Closeable {
    /**
     * Insert row into table
     *
     * @param row Row to be inserted
     */
    void insertRow(Row row);

    /**
     * Update row in table
     *
     * @param oldRow         The old row
     * @param newRow         The new row
     * @param changedIndices List of indices with updated values.
     * @throws com.nearinfinity.honeycomb.exceptions.RowNotFoundException
     *
     */
    void updateRow(Row oldRow, Row newRow, Collection<IndexSchema> changedIndices);

    /**
     * Remove row from the table
     *
     * @param row The row to be deleted
     * @throws com.nearinfinity.honeycomb.exceptions.RowNotFoundException
     *
     */
    void deleteRow(Row row);

    /**
     * Inserts an index on the table
     *
     * @param indexSchema The {@link com.nearinfinity.honeycomb.mysql.schema.IndexSchema} representing the index details, not null
     */
    void insertTableIndex(final IndexSchema indexSchema);

    /**
     * Deletes the index corresponding to the specified index name from the table
     *
     * @param indexSchema The {@link com.nearinfinity.honeycomb.mysql.schema.IndexSchema} representing the index details, not null
     */
    void deleteTableIndex(final IndexSchema indexSchema);

    /**
     * Remove all rows from the table.
     */
    void deleteAllRows();

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
    Row getRow(UUID uuid);

    /**
     * Create a scanner for an unordered full table scan
     *
     * @return Scanner over table
     */
    Scanner tableScan();

    /**
     * Return a scanner over the full index in ascending sort order.
     *
     * @param key
     * @return Scanner over index
     */
    Scanner ascendingIndexScan(QueryKey key);

    /**
     * Return a scanner over the table's index at the key in ascending sort order.
     *
     * @param key
     * @return Scanner over index
     */
    Scanner ascendingIndexScanAt(QueryKey key);

    /**
     * Return a scanner over the table's index after the specified key in
     * ascending sort order.
     *
     * @param key
     * @return Scanner over index
     */
    Scanner ascendingIndexScanAfter(QueryKey key);

    /**
     * Return a scanner over the full index in descending sort order.
     *
     * @param key
     * @return Scanner over index
     */
    Scanner descendingIndexScan(QueryKey key);

    /**
     * Return a scanner over the table's index at the specified key in descending
     * sort order.
     *
     * @param key
     * @return Scanner over index
     */
    Scanner descendingIndexScanAt(QueryKey key);

    /**
     * Return a scanner over the table's index after the specified key in
     * descending sort.
     *
     * @param key
     * @return Scanner over index
     */
    Scanner descendingIndexScanBefore(QueryKey key);

    /**
     * Return a scanner over the rows in the table with the specified key
     *
     * @param key
     * @return Scanner over index
     */
    Scanner indexScanExact(QueryKey key);
}