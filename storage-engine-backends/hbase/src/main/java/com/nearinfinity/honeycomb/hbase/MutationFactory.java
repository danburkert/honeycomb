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


package com.nearinfinity.honeycomb.hbase;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.nearinfinity.honeycomb.hbase.config.ConfigConstants;
import com.nearinfinity.honeycomb.hbase.rowkey.DataRowKey;
import com.nearinfinity.honeycomb.hbase.rowkey.IndexRowKeyBuilder;
import com.nearinfinity.honeycomb.hbase.rowkey.RowKey;
import com.nearinfinity.honeycomb.hbase.rowkey.SortOrder;
import com.nearinfinity.honeycomb.mysql.Row;
import com.nearinfinity.honeycomb.mysql.schema.IndexSchema;
import com.nearinfinity.honeycomb.mysql.schema.TableSchema;
import com.nearinfinity.honeycomb.util.Verify;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Creates put and deleteRow lists for various operations.  Meant to have no
 * side effects except for requesting metadata from the store.
 */
public class MutationFactory {
    private final HBaseStore store;
    private byte[] columnFamily;

    @Inject
    public MutationFactory(HBaseStore store) {
        super();
        this.store = store;
    }

    /**
     * Sets the column family.  Cannot be injected into the constructor directly
     * because of a bug in Cobertura.  Called automatically by Guice.
     *
     * @param columnFamily The column family to use
     */
    @Inject
    public void setColumnFamily(final @Named(ConfigConstants.COLUMN_FAMILY) String columnFamily) {
        this.columnFamily = columnFamily.getBytes();
    }

    /**
     * Build put list for a row insert with indices
     *
     * @param tableId
     * @param row
     * @return The list of put mutations
     */
    public List<Put> insert(long tableId, final Row row) {
        return insert(tableId, row, store.getSchema(tableId).getIndices());
    }

    /**
     * Build put list for a row insert with specified indices
     *
     * @param tableId
     * @param row
     * @param indices
     * @return The list of put mutations
     */
    public List<Put> insert(long tableId, final Row row,
                            final Collection<IndexSchema> indices) {
        checkNotNull(row);
        // tableId, indices checked by called methods

        final byte[] serializedRow = row.serialize();
        final UUID uuid = row.getUUID();
        final ImmutableList.Builder<Put> puts = ImmutableList.builder();

        puts.add(emptyQualifierPut(new DataRowKey(tableId, uuid), serializedRow));
        puts.addAll(insertIndices(tableId, row, indices));

        return puts.build();
    }

    /**
     * Build put list for inserting only the specified indices of the row
     *
     * @param tableId
     * @param row
     * @param indices
     * @return The list of put mutations
     */
    public List<Put> insertIndices(long tableId, final Row row,
                                   final Collection<IndexSchema> indices) {
        checkNotNull(row);
        final byte[] serializedRow = row.serialize();
        final ImmutableList.Builder<Put> puts = ImmutableList.builder();
        doToIndices(tableId, row, indices, new IndexAction() {
            @Override
            public void execute(IndexRowKeyBuilder builder) {
                puts.add(emptyQualifierPut(builder.withSortOrder(SortOrder.Ascending).build(), serializedRow));
                puts.add(emptyQualifierPut(builder.withSortOrder(SortOrder.Descending).build(), serializedRow));
            }
        });
        return puts.build();
    }

    /**
     * Build delete list for the data and indices belonging to the row
     *
     * @param tableId
     * @param row
     * @return The list of delete mutations
     */
    public List<Delete> delete(long tableId, final Row row) {
        List<Delete> deletes = deleteIndices(tableId, row);
        deletes.add(new Delete(new DataRowKey(tableId, row.getUUID()).encode()));
        return deletes;
    }

    /**
     * Build delete list for the indices belonging to the row
     *
     * @param tableId
     * @param row
     * @return The list of delete mutations
     */
    public List<Delete> deleteIndices(long tableId, final Row row) {
        Verify.isValidId(tableId);

        final Collection<IndexSchema> indices = store.getSchema(tableId).getIndices();
        return deleteIndices(tableId, row, indices);
    }

    /**
     * Build delete list for the specified indices belonging to the row
     *
     * @param tableId
     * @param row
     * @param indices
     * @return The list of delete mutations
     */
    public List<Delete> deleteIndices(long tableId, final Row row,
                                      final Collection<IndexSchema> indices) {
        Verify.isValidId(tableId);
        checkNotNull(row);

        final List<Delete> deletes = Lists.newLinkedList();
        doToIndices(tableId, row, indices, new IndexAction() {
            @Override
            public void execute(IndexRowKeyBuilder builder) {
                deletes.add(new Delete(builder.withSortOrder(SortOrder.Ascending).build().encode()));
                deletes.add(new Delete(builder.withSortOrder(SortOrder.Descending).build().encode()));
            }
        });
        return deletes;
    }

    private Put emptyQualifierPut(final RowKey rowKey,
                                         final byte[] serializedRow) {
        return new Put(rowKey.encode()).add(columnFamily,
                new byte[0], serializedRow);
    }

    private void doToIndices(long tableId,
                             final Row row,
                             final Collection<IndexSchema> indices,
                             final IndexAction action) {

        for (IndexSchema index : indices) {
            long indexId = store.getIndexId(tableId, index.getIndexName());
            TableSchema schema = store.getSchema(tableId);

            IndexRowKeyBuilder builder = IndexRowKeyBuilder
                    .newBuilder(tableId, indexId)
                    .withUUID(row.getUUID())
                    .withRow(row, index.getIndexName(), schema);
            action.execute(builder);
        }
    }

    private interface IndexAction {
        public void execute(IndexRowKeyBuilder builder);
    }
}