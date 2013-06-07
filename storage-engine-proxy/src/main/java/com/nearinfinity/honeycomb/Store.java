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
 * Copyright 2013 Near Infinity Corporation.
 */


package com.nearinfinity.honeycomb;

import com.nearinfinity.honeycomb.mysql.schema.IndexSchema;
import com.nearinfinity.honeycomb.mysql.schema.TableSchema;

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
     * Create a table
     *
     * @param tableName
     * @param schema
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
     * Add the provided index information to the table specified by the table name
     *
     * @param tableName The name of the table to be altered, not null or empty
     * @param schema    The schema of the index, not null
     */
    public void addIndex(String tableName, IndexSchema schema);

    /**
     * Drop the specified index from the table specified by the table name
     *
     * @param tableName The name of the table to be altered, not null or empty
     * @param indexName The name of the index to be dropped, not null or empty
     */
    public void dropIndex(String tableName, String indexName);

    /**
     * Gets the current value of the auto increment column in the table
     *
     * @param tableName
     * @return The current auto-increment value
     */
    public long getAutoInc(String tableName);

    /**
     * Set the table's auto increment value to the greater of value and the
     * table's current auto increment value.
     *
     * @param tableName
     * @param value
     */
    public void setAutoInc(String tableName, long value);

    /**
     * Increment the table's auto increment value by amount, and return the new
     * value (post increment).
     *
     * @param tableName Name of table
     * @param amount    Amount to auto increment by
     * @return The post-incremented value
     */
    public long incrementAutoInc(String tableName, long amount);

    /**
     * Reset the table's auto increment value to 1.
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
