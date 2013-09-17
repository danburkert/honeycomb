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


package com.nearinfinity.honeycomb.hbase;

import com.google.common.base.Charsets;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.nearinfinity.honeycomb.config.Constants;
import com.nearinfinity.honeycomb.exceptions.MetadataNotFoundException;
import com.nearinfinity.honeycomb.exceptions.RuntimeIOException;
import com.nearinfinity.honeycomb.exceptions.TableExistsException;
import com.nearinfinity.honeycomb.exceptions.TableNotFoundException;
import com.nearinfinity.honeycomb.hbase.config.HBaseProperties;
import com.nearinfinity.honeycomb.mysql.schema.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.schema.IndexSchema;
import com.nearinfinity.honeycomb.mysql.schema.TableSchema;
import com.nearinfinity.honeycomb.util.Verify;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Manages writing and reading table & column schemas, table & column ids, and
 * row & autoincrement counters to and from HBase.
 */
public class MultiTableHBaseMetadata {
    private final HTablePool tablePool;
    private byte[] columnFamily;
    private byte[] DIRECTORY_TABLE = encodeString(".HONEYCOMB.");

    private static final byte[] idQualifier = encodeString("table-id");
    private static final byte[] columnsQualifier = encodeString("columns");
    private static final byte[] indicesQualifier = encodeString("indices");
    private static final byte[] schemaQualifier = encodeString("schema");
    private static final byte[] autoIncrementCounterQualifier = encodeString("auto-increment-counter");
    private static final byte[] columnCounterQualifier = encodeString("column-counter");
    private static final byte[] indexCounterQualifier = encodeString("index-counter");


    @Inject
    public MultiTableHBaseMetadata(final HTablePool tablePool, Configuration configuration) {
        this.tablePool = tablePool;
        this.columnFamily = configuration.get(HBaseProperties.COLUMN_FAMILY).getBytes();
    }

    public String getTableId(String tableName) {
        HTableInterface hTable = getDirectoryTable();
        try {
            Get get = new Get(encodeString(tableName));
            get.addColumn(columnFamily, idQualifier);
            Result result = HBaseOperations.performGet(hTable, get);
            byte[] tableId = result.getValue(columnFamily, idQualifier);
            if (tableId == null) {
                throw new TableNotFoundException(tableName);
            }
            return decodeString(tableId);
        } finally {
            HBaseOperations.closeTable(hTable);
        }
    }

    /**
     * Fetches the index name to index identifier bimap for the specified table
     *
     * @param tableName The table name
     * @return The indices mapping details for the table
     */
    public BiMap<String, Long> getIndexIds(String tableName) {
        return getIdMap(tableName, indicesQualifier);
    }

    /**
     * Fetches the column name to column identifier bimap for the specified table
     *
     * @param tableName The table name
     * @return The columns mapping details for the table
     */
    public BiMap<String, Long> getColumnIds(final String tableName) {
        return getIdMap(tableName, columnsQualifier);
    }

    /**
     * Fetches the {@link com.nearinfinity.honeycomb.mysql.schema.TableSchema} for the table corresponding to the specified table identifier
     *
     * @param tableName The table name
     * @return The table schema details for the table
     */
    public TableSchema getSchema(final String tableName) {
        final byte[] rowkey = encodeString(tableName);
        final Get get = new Get(rowkey);
        get.addColumn(columnFamily, schemaQualifier);

        final HTableInterface hTable = getDirectoryTable();

        try {
            final Result result = HBaseOperations.performGet(hTable, get);

            final byte[] serializedSchema = result.getValue(columnFamily, schemaQualifier);
            if (serializedSchema == null) {
                throw new TableNotFoundException(tableName);
            }
            return TableSchema.deserialize(serializedSchema);
        } finally {
            HBaseOperations.closeTable(hTable);
        }
    }

    /**
     * Performs all metadata operations necessary to create a table
     *
     * @param tableName The name of the table to create, not null or empty
     * @param schema    The schema details of the table to create, not null
     */
    public void createTable(final String tableName, final TableSchema schema) {
        Verify.isNotNullOrEmpty(tableName);

        byte[] rowkey = encodeString(tableName);
        Put put = new Put(rowkey);

        BiMap<String, Long> columns = HashBiMap.create();
        long columnCounter = 0;
        for (ColumnSchema columnSchema : schema.getColumns()) {
            columns.put(columnSchema.getColumnName(), columnCounter++);
        }
        put.add(columnFamily, columnsQualifier, IdMap.serialize(columns));
        put.add(columnFamily, columnCounterQualifier, Bytes.toBytes(columnCounter));

        if (schema.hasIndices()) {
            BiMap<String, Long> indices = HashBiMap.create();
            long indexCounter = 0;
            for (IndexSchema indexSchema : schema.getIndices()) {
                indices.put(indexSchema.getIndexName(), columnCounter++);
            }
            put.add(columnFamily, indicesQualifier, IdMap.serialize(indices));
            put.add(columnFamily, indexCounterQualifier, Bytes.toBytes(indexCounter));
        }

        put.add(columnFamily, schemaQualifier, schema.serialize());

        HTableInterface hTable = getDirectoryTable();

        try {
            if (!HBaseOperations.performCheckAndPut(hTable, rowkey, columnFamily, schemaQualifier, null, put)) {
                throw new TableExistsException(tableName);
            }
        } finally {
            HBaseOperations.performFlush(hTable);
            HBaseOperations.closeTable(hTable);
        }

    }

    /**
     * Performs all metadata operations necessary to create a table index
     *
     * @param tableName   The name of the table
     * @param indexSchema The {@link com.nearinfinity.honeycomb.mysql.schema.IndexSchema} representing the index details, not null
     */
    public void createTableIndex(final String tableName,
                                 final IndexSchema indexSchema) {
        checkNotNull(indexSchema, "The index schema is invalid");

        final byte[] rowkey = encodeString(tableName);
        final Get metadataGet = new Get(rowkey);
        metadataGet.addColumn(columnFamily, schemaQualifier);
        metadataGet.addColumn(columnFamily, indicesQualifier);

        final HTableInterface hTable = getDirectoryTable();

        try {
            final Result metadataResult = HBaseOperations.performGet(hTable, metadataGet);
            long indexId = HBaseOperations.performIncrementCounter(hTable, rowkey, columnFamily, indexCounterQualifier, 1l);

            final byte[] serializedSchema = metadataResult.getValue(columnFamily, schemaQualifier);
            if (serializedSchema == null) {
                throw new TableNotFoundException(tableName);
            }
            TableSchema schema = TableSchema.deserialize(serializedSchema);
            schema.addIndices(ImmutableList.of(indexSchema));

            final byte[] serializedIndices = metadataResult.getValue(columnFamily, indicesQualifier);
            if (serializedIndices == null) {
                throw new MetadataNotFoundException(tableName, decodeString(indicesQualifier));
            }
            BiMap<String, Long> indices = HashBiMap.create(IdMap.deserialize(serializedIndices));
            indices.put(indexSchema.getIndexName(), indexId);

            Put put = new Put(rowkey);
            put.add(columnFamily, schemaQualifier, schema.serialize());
            put.add(columnFamily, indicesQualifier, IdMap.serialize(indices));

            boolean success = HBaseOperations.performCheckAndPut(hTable, rowkey, columnFamily, schemaQualifier, serializedSchema, put);
            if (!success) {
                throw new RuntimeIOException(new IOException("Concurrent modification to schema of table " + tableName));
            }
        } finally {
            HBaseOperations.performFlush(hTable);
            HBaseOperations.closeTable(hTable);
        }
    }

    /**
     * Performs all metadata operations necessary to remove the specified index from the specified table
     *
     * @param tableName The table name
     * @param indexName The identifying name of the index, not null or empty
     */
    public void deleteTableIndex(final String tableName, final String indexName) {
        Verify.isNotNullOrEmpty(indexName, "The index name is invalid");

        final byte[] rowkey = encodeString(tableName);
        final Get metadataGet = new Get(rowkey);
        metadataGet.addColumn(columnFamily, schemaQualifier);
        metadataGet.addColumn(columnFamily, indicesQualifier);

        final HTableInterface hTable = getDirectoryTable();

        try {
            final Result metadataResult = HBaseOperations.performGet(hTable, metadataGet);

            final byte[] serializedSchema = metadataResult.getValue(columnFamily, schemaQualifier);
            if (serializedSchema == null) {
                throw new TableNotFoundException(tableName);
            }
            TableSchema schema = TableSchema.deserialize(serializedSchema);
            schema.removeIndex(indexName);

            final byte[] serializedIndices = metadataResult.getValue(columnFamily, indicesQualifier);
            if (serializedIndices == null) {
                throw new MetadataNotFoundException(tableName, decodeString(indicesQualifier));
            }
            BiMap<String, Long> indices = HashBiMap.create(IdMap.deserialize(serializedIndices));
            indices.remove(indexName);

            Put put = new Put(rowkey);
            put.add(columnFamily, schemaQualifier, schema.serialize());
            put.add(columnFamily, indicesQualifier, IdMap.serialize(indices));

            boolean success = HBaseOperations.performCheckAndPut(hTable, rowkey, columnFamily, schemaQualifier, serializedSchema, put);
            if (!success) {
                throw new RuntimeIOException(new IOException("Concurrent modification to schema of table " + tableName));
            }
        } finally {
            HBaseOperations.performFlush(hTable);
            HBaseOperations.closeTable(hTable);
        }
    }

    /**
     * Performs all metadata operations necessary to drop the specified table from
     * the directory.
     *
     * @param tableName The name of the table to drop, not null or empty
     */
    public void dropTable(String tableName) {
        Verify.isNotNullOrEmpty(tableName);

        Delete delete = new Delete(encodeString(tableName));
        delete.deleteFamily(columnFamily);
        HTableInterface hTable = getDirectoryTable();

        try {
            HBaseOperations.performDelete(hTable, delete);
        } finally {
            HBaseOperations.performFlush(hTable);
            HBaseOperations.closeTable(hTable);
        }
    }

    /**
     * Performs the operations necessary to rename a table
     *
     * @param oldTableName The name of the existing table
     * @param newTableName The new name to use for this table
     * @throws com.nearinfinity.honeycomb.exceptions.TableNotFoundException Thrown when existing table cannot be found
     */
    public void renameTable(final String oldTableName, final String newTableName) {
        Verify.isNotNullOrEmpty(oldTableName, "Old table name must have value");
        Verify.isNotNullOrEmpty(newTableName, "New table name must have value");

        byte[] oldRowKey = encodeString(oldTableName);
        byte[] newRowKey = encodeString(newTableName);

        Get metadataGet = new Get(oldRowKey);
        metadataGet.addFamily(columnFamily);

        HTableInterface hTable = getDirectoryTable();

        try {
            Result metadataResult = HBaseOperations.performGet(hTable, metadataGet);
            Put put = new Put(newRowKey);
            for (Map.Entry<byte[], byte[]> entry : metadataResult.getNoVersionMap().get(columnFamily).entrySet()) {
                put.add(columnFamily, entry.getKey(), entry.getValue());
            }

            // Check that an existing table does not exist with the new name
            if(HBaseOperations.performCheckAndPut(hTable, newRowKey, columnFamily, schemaQualifier, null, put)) {
                Delete delete = new Delete(oldRowKey).deleteFamily(columnFamily);
                boolean success = HBaseOperations.performCheckAndDelete(hTable, oldRowKey, columnFamily,
                        schemaQualifier, metadataResult.getValue(columnFamily, schemaQualifier),
                        delete);
                if(!success) {
                    // Directory is in a dirty state; delete the
                    // new table before throwing exception.
                    dropTable(newTableName);
                    throw new IllegalStateException("Table being renamed has been modified during the rename.");
                }
            } else {
                throw new TableExistsException(newTableName);
            }

        } finally {
            HBaseOperations.performFlush(hTable);
            HBaseOperations.closeTable(hTable);
        }
    }

    /**
     * Retrieve the current auto increment value for a table by its ID.
     *
     * @param tableName Table ID
     * @return Auto increment value
     */
    public long getAutoInc(final String tableName) {
        return getCounter(encodeString(tableName), autoIncrementCounterQualifier);
    }

    /**
     * Increment a table's autoincrement value by an amount
     *
     * @param tableName Table name
     * @param amount  Increment amount
     * @return New auto increment value
     */
    public long incrementAutoInc(final String tableName, long amount) {
        long tableId = 0;
        Verify.isValidId(tableId);
        return incrementCounter(encodeString(tableName), autoIncrementCounterQualifier, amount);
    }

    /**
     * Set a table's autoincrement value to a specified value
     *
     * @param tableName Table name
     * @param value   New autoincrement value
     */
    public void setAutoInc(final String tableName, long value) {
        Put put = new Put(encodeString(tableName));
        put.add(columnFamily, autoIncrementCounterQualifier, Bytes.toBytes(value));
        HTableInterface hTable = getDirectoryTable();
        try {
            HBaseOperations.performPut(hTable, put);
        } finally {
            HBaseOperations.closeTable(hTable);
        }
    }

    private BiMap<String, Long> getIdMap(String tableName, byte[] metadata) {
        HTableInterface hTable = getDirectoryTable();
        try {
            Get get = new Get(encodeString(tableName));
            get.addColumn(columnFamily, metadata);
            Result result = HBaseOperations.performGet(hTable, get);
            if (result.isEmpty()) {
                throw new MetadataNotFoundException(tableName, decodeString(metadata));
            }
            return IdMap.deserialize(result.getValue(columnFamily, metadata));
        } finally {
            HBaseOperations.closeTable(hTable);
        }
    }

    private long getCounter(byte[] row, byte[] qualifier) {
        Get get = new Get(row).addColumn(columnFamily, qualifier);
        HTableInterface hTable = getDirectoryTable();
        try {
            byte[] value = HBaseOperations.performGet(hTable, get).getValue(columnFamily, qualifier);
            return value == null ? 0 : Bytes.toLong(value);
        } finally {
            HBaseOperations.closeTable(hTable);
        }
    }

    private long incrementCounter(final byte[] row, final byte[] qualifier, final long amount) {
        final HTableInterface hTable = getDirectoryTable();
        try {
            return HBaseOperations.performIncrementCounter(hTable, row, columnFamily, qualifier, amount);
        } finally {
            HBaseOperations.closeTable(hTable);
        }
    }

    private static byte[] encodeString(String string) {
        return string.getBytes(Charsets.UTF_8);
    }

    private static String decodeString(byte[] string) {
        return new String(string, Charsets.UTF_8);
    }

    private HTableInterface getDirectoryTable() {
        return tablePool.getTable(DIRECTORY_TABLE);
    }
}