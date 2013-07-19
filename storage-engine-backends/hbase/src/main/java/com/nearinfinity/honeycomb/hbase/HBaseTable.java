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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.name.Named;
import com.nearinfinity.honeycomb.Scanner;
import com.nearinfinity.honeycomb.Table;
import com.nearinfinity.honeycomb.exceptions.RowNotFoundException;
import com.nearinfinity.honeycomb.hbase.config.HBaseProperties;
import com.nearinfinity.honeycomb.hbase.rowkey.DataRowKey;
import com.nearinfinity.honeycomb.hbase.rowkey.IndexRowKey;
import com.nearinfinity.honeycomb.hbase.rowkey.IndexRowKeyBuilder;
import com.nearinfinity.honeycomb.hbase.rowkey.SortOrder;
import com.nearinfinity.honeycomb.mysql.QueryKey;
import com.nearinfinity.honeycomb.mysql.Row;
import com.nearinfinity.honeycomb.mysql.Util;
import com.nearinfinity.honeycomb.mysql.schema.IndexSchema;
import com.nearinfinity.honeycomb.mysql.schema.TableSchema;
import com.nearinfinity.honeycomb.util.Verify;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.coprocessor.example.BulkDeleteProtocol;
import org.apache.hadoop.hbase.coprocessor.example.BulkDeleteResponse;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An HBase backed {@link Table}
 */
public class HBaseTable implements Table {
    private final HTableInterface hTable;
    private final HBaseStore store;
    private final long tableId;
    private final MutationFactory mutationFactory;
    private long writeBufferSize;
    private String columnFamily;

    @Inject
    public HBaseTable(HTableInterface hTable, HBaseStore store, MutationFactory mutationFactory, @Assisted Long tableId) {
        Verify.isValidId(tableId);
        this.hTable = checkNotNull(hTable);
        this.store = checkNotNull(store);
        this.tableId = tableId;
        this.mutationFactory = mutationFactory;
    }

    private static byte[] incrementRowKey(byte[] key) {
        return new BigInteger(key).add(BigInteger.ONE).toByteArray();
    }

    /**
     * Sets the write buffer size.  Cannot be injected into the constructor directly
     * because of a bug in Cobertura.  Called automatically by Guice.
     *
     * @param bufferSize
     */
    @Inject
    public void setWriterBufferSize(final @Named(HBaseProperties.WRITE_BUFFER) Long bufferSize) {
        writeBufferSize = bufferSize;
    }

    /**
     * Sets the column family.  Cannot be injected into the constructor directly
     * because of a bug in Cobertura.  Called automatically by Guice.
     *
     * @param columnFamily The column family to use
     */
    @Inject
    public void setColumnFamily(final @Named(HBaseProperties.COLUMN_FAMILY) String columnFamily) {
        this.columnFamily = columnFamily;
    }

    @Override
    public void insertRow(Row row) {
        checkNotNull(row);
        HBaseOperations.performPut(hTable, mutationFactory.insert(tableId, row));
    }

    @Override
    public void insertTableIndex(final IndexSchema indexSchema) {
        checkNotNull(indexSchema, "The index schema is invalid");
        final Collection<IndexSchema> indices = ImmutableList.of(indexSchema);
        final Scanner scanner = tableScan();
        TableSchema schema = store.getSchema(tableId);
        while (scanner.hasNext()) {
            HBaseOperations.performPut(hTable,
                    mutationFactory.insertIndices(tableId, Row.deserialize(scanner.next(), schema), indices));
        }

        Util.closeQuietly(scanner);
    }

    @Override
    public void deleteTableIndex(final IndexSchema indexSchema) {
        checkNotNull(indexSchema, "The index schema is invalid");

        long indexId = store.getIndexId(tableId, indexSchema.getIndexName());

        deleteRowsInRange(
                IndexRowKeyBuilder.newBuilder(tableId, indexId).withSortOrder(SortOrder.Ascending).build().encode(),
                IndexRowKeyBuilder.newBuilder(tableId, nextAscendingValue(indexId)).withSortOrder(SortOrder.Ascending).build().encode());
        deleteRowsInRange(
                IndexRowKeyBuilder.newBuilder(tableId, indexId).withSortOrder(SortOrder.Descending).build().encode(),
                IndexRowKeyBuilder.newBuilder(tableId, nextDescendingValue(indexId)).withSortOrder(SortOrder.Descending).build().encode());
    }

    @Override
    public void updateRow(Row oldRow, Row newRow, Collection<IndexSchema> changedIndices) {
        checkNotNull(newRow);

        // Delete indices that have changed
        final List<Delete> deletes =
                mutationFactory.deleteIndices(tableId, oldRow, changedIndices);
        // Insert data row and indices that have changed
        final List<Put> puts =
                mutationFactory.insert(tableId, newRow);

        HBaseOperations.performDelete(hTable, deletes);
        HBaseOperations.performPut(hTable, puts);
    }

    @Override
    public void deleteRow(final Row row) {
        checkNotNull(row);
        HBaseOperations.performDelete(hTable, mutationFactory.delete(tableId, row));
    }

    @Override
    public void deleteAllRows() {
        deleteRowsInRange(new DataRowKey(tableId).encode(), new DataRowKey(tableId + 1).encode());
        deleteRowsInRange(
                IndexRowKeyBuilder.createDropTableIndexKey(tableId, SortOrder.Ascending),
                IndexRowKeyBuilder.createDropTableIndexKey(nextAscendingValue(tableId), SortOrder.Ascending));
        deleteRowsInRange(
                IndexRowKeyBuilder.createDropTableIndexKey(tableId, SortOrder.Descending),
                IndexRowKeyBuilder.createDropTableIndexKey(nextDescendingValue(tableId), SortOrder.Descending));
    }

    @Override
    public void flush() {
        HBaseOperations.performFlush(hTable);
    }

    @Override
    public Row getRow(UUID uuid) {
        DataRowKey dataRow = new DataRowKey(tableId, uuid);
        Get get = new Get(dataRow.encode());
        Result result = HBaseOperations.performGet(hTable, get);
        if (result.isEmpty()) {
            throw new RowNotFoundException(uuid);
        }
        return Row.deserialize(result.getValue(columnFamily.getBytes(), new byte[0]), store.getSchema(tableId));
    }

    @Override
    public Scanner tableScan() {
        DataRowKey startRow = new DataRowKey(tableId);
        DataRowKey endRow = new DataRowKey(nextAscendingValue(tableId));
        return createScannerForRange(startRow.encode(), endRow.encode());
    }

    @Override
    public Scanner ascendingIndexScan(QueryKey key) {
        long indexId = store.getIndexId(tableId, key.getIndexName());

        IndexRowKey startRow = IndexRowKeyBuilder
                .newBuilder(tableId, indexId)
                .withSortOrder(SortOrder.Ascending)
                .build();

        IndexRowKey endRow = IndexRowKeyBuilder
                .newBuilder(tableId, nextAscendingValue(indexId))
                .withSortOrder(SortOrder.Ascending)
                .build();

        return createScannerForRange(startRow.encode(), endRow.encode());
    }

    @Override
    public Scanner ascendingIndexScanAt(QueryKey key) {
        final TableSchema schema = store.getSchema(tableId);
        long indexId = store.getIndexId(tableId, key.getIndexName());

        IndexRowKey startRow = IndexRowKeyBuilder
                .newBuilder(tableId, indexId)
                .withQueryKey(key, schema)
                .withSortOrder(SortOrder.Ascending)
                .build();

        IndexRowKey endRow = IndexRowKeyBuilder
                .newBuilder(tableId, nextAscendingValue(indexId))
                .withSortOrder(SortOrder.Ascending)
                .build();

        return createScannerForRange(startRow.encode(), endRow.encode());
    }

    @Override
    public Scanner ascendingIndexScanAfter(QueryKey key) {
        final TableSchema schema = store.getSchema(tableId);
        long indexId = store.getIndexId(tableId, key.getIndexName());

        IndexRowKey startRow = IndexRowKeyBuilder
                .newBuilder(tableId, indexId)
                .withQueryKey(key, schema)
                .withSortOrder(SortOrder.Ascending)
                .build();

        IndexRowKey endRow = IndexRowKeyBuilder.newBuilder(tableId, nextAscendingValue(indexId))
                .withSortOrder(SortOrder.Ascending)
                .build();

        return createScannerForRange(incrementRowKey(startRow.encode()), endRow.encode());
    }

    @Override
    public Scanner descendingIndexScan(QueryKey key) {
        long indexId = store.getIndexId(tableId, key.getIndexName());

        IndexRowKey startRow = IndexRowKeyBuilder.newBuilder(tableId, indexId)
                .withSortOrder(SortOrder.Descending)
                .build();

        IndexRowKey endRow = IndexRowKeyBuilder.newBuilder(tableId, nextDescendingValue(indexId))
                .withSortOrder(SortOrder.Descending)
                .build();

        return createScannerForRange(startRow.encode(), endRow.encode());
    }

    @Override
    public Scanner descendingIndexScanAt(QueryKey key) {
        final TableSchema schema = store.getSchema(tableId);
        long indexId = store.getIndexId(tableId, key.getIndexName());

        IndexRowKey startRow = IndexRowKeyBuilder
                .newBuilder(tableId, indexId)
                .withQueryKey(key, schema)
                .withSortOrder(SortOrder.Descending)
                .build();

        IndexRowKey endRow = IndexRowKeyBuilder
                .newBuilder(tableId, nextDescendingValue(indexId))
                .withSortOrder(SortOrder.Descending)
                .build();

        return createScannerForRange(startRow.encode(), endRow.encode());
    }

    @Override
    public Scanner descendingIndexScanBefore(QueryKey key) {
        final TableSchema schema = store.getSchema(tableId);
        long indexId = store.getIndexId(tableId, key.getIndexName());

        IndexRowKey startRow = IndexRowKeyBuilder
                .newBuilder(tableId, indexId)
                .withQueryKey(key, schema)
                .withSortOrder(SortOrder.Descending)
                .build();

        IndexRowKey endRow = IndexRowKeyBuilder
                .newBuilder(tableId, nextDescendingValue(indexId))
                .withSortOrder(SortOrder.Descending)
                .build();

        return createScannerForRange(incrementRowKey(startRow.encode()), endRow.encode());
    }

    @Override
    public Scanner indexScanExact(QueryKey key) {
        final TableSchema schema = store.getSchema(tableId);
        long indexId = store.getIndexId(tableId, key.getIndexName());

        IndexRowKey row = IndexRowKeyBuilder
                .newBuilder(tableId, indexId)
                .withQueryKey(key, schema)
                .withSortOrder(SortOrder.Ascending)
                .build();

        // Scan is [start, end) : increment to set end to next possible row
        return createScannerForRange(row.encode(), incrementRowKey(row.encode()));
    }

    @Override
    public void close() {
        Util.closeQuietly(hTable);
    }

    /**
     * Delete rows in specified range.  Requires the BulkDeleteEndpoint
     * coprocessor to be installed on each regionserver serving regions within
     * the range.
     */
    private void deleteRowsInRange(byte[] start, byte[] end) {
        final Scan scan = new Scan(start, end).setFilter(
                new FilterList(
                        new FirstKeyOnlyFilter(),
                        new KeyOnlyFilter()));

        try {
            hTable.coprocessorExec(
                    BulkDeleteProtocol.class, start, end, new Batch.Call<BulkDeleteProtocol, BulkDeleteResponse>() {
                @Override
                public BulkDeleteResponse call(BulkDeleteProtocol instance) throws IOException {
                    return instance.delete(scan, BulkDeleteProtocol.DeleteType.ROW, Long.MAX_VALUE, (int) writeBufferSize);
                }
            });
        } catch (Throwable throwable) {
            throwable.printStackTrace();
            throw new RuntimeException(throwable);
        }
    }

    private Scanner createScannerForRange(byte[] start, byte[] end) {
        Scan scan = new Scan(start, end);
        ResultScanner scanner = HBaseOperations.getScanner(hTable, scan);
        return new HBaseScanner(scanner, columnFamily);
    }

    private long nextDescendingValue(long value) {
        return value - 1;
    }

    private long nextAscendingValue(long value) {
        return value + 1;
    }
}

