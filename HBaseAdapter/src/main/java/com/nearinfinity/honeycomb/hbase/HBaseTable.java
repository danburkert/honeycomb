package com.nearinfinity.honeycomb.hbase;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.nearinfinity.honeycomb.RowNotFoundException;
import com.nearinfinity.honeycomb.Scanner;
import com.nearinfinity.honeycomb.Table;
import com.nearinfinity.honeycomb.hbase.rowkey.DataRow;
import com.nearinfinity.honeycomb.hbase.rowkey.IndexRow;
import com.nearinfinity.honeycomb.hbase.rowkey.IndexRowBuilder;
import com.nearinfinity.honeycomb.hbase.rowkey.SortOrder;
import com.nearinfinity.honeycomb.hbaseclient.Constants;
import com.nearinfinity.honeycomb.mysql.IndexKey;
import com.nearinfinity.honeycomb.mysql.Row;
import com.nearinfinity.honeycomb.mysql.Util;
import com.nearinfinity.honeycomb.mysql.gen.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.gen.ColumnType;
import com.nearinfinity.honeycomb.mysql.gen.IndexSchema;
import com.nearinfinity.honeycomb.mysql.gen.TableSchema;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class HBaseTable implements Table {
    private final HTableInterface hTable;
    private final HBaseStore store;
    private final long tableId;
    private final TableSchema schema;

    @Inject
    public HBaseTable(HTableInterface hTable, HBaseStore store,
                      @Assisted Long tableId, @Assisted TableSchema schema) {
        this.hTable = hTable;
        this.store = store;
        this.tableId = tableId;
        this.schema = schema;
    }

    @Override
    public void insert(Row row) {
        final byte[] serializeRow = row.serialize();
        final UUID uuid = row.getUUID();

        HBaseOperations.performPut(hTable, createEmptyQualifierPut(new DataRow(this.tableId, uuid), serializeRow));
        doToIndices(row, new IndexAction() {
            @Override
            public void execute(IndexRowBuilder builder) {
                HBaseOperations.performPut(hTable, createEmptyQualifierPut(builder.withSortOrder(SortOrder.Ascending).build(), serializeRow));
                HBaseOperations.performPut(hTable, createEmptyQualifierPut(builder.withSortOrder(SortOrder.Descending).build(), serializeRow));
            }
        });
    }

    @Override
    public void update(Row row) {
        this.delete(row.getUUID());
        this.insert(row);
    }

    @Override
    public void delete(final UUID uuid) {
        Row row = this.get(uuid);
        final List<Delete> deleteList = Lists.newLinkedList();
        deleteList.add(new Delete(new DataRow(this.tableId, uuid).encode()));
        doToIndices(row, new IndexAction() {
            @Override
            public void execute(IndexRowBuilder builder) {
                deleteList.add(createEmptyQualifierDelete(builder.withSortOrder(SortOrder.Descending).build()));
                deleteList.add(createEmptyQualifierDelete(builder.withSortOrder(SortOrder.Ascending).build()));
            }
        });

        HBaseOperations.performDelete(hTable, deleteList);
    }

    @Override
    public void deleteAllRows() {
        long totalDeleteSize = 0, writeBufferSize = 50000; // TODO: retrieve write buffer size from configuration
        final List<Delete> deleteList = Lists.newLinkedList();
        Scanner rows = this.tableScan();
        while (rows.hasNext()) {
            Row row = rows.next();
            final UUID uuid = row.getUUID();
            deleteList.add(new Delete(new DataRow(this.tableId, uuid).encode()));
            doToIndices(row, new IndexAction() {
                @Override
                public void execute(IndexRowBuilder builder) {
                    deleteList.add(createEmptyQualifierDelete(builder.withSortOrder(SortOrder.Ascending).build()));
                    deleteList.add(createEmptyQualifierDelete(builder.withSortOrder(SortOrder.Descending).build()));
                }
            });

            totalDeleteSize += deleteList.size() * row.serialize().length;
            if (totalDeleteSize > writeBufferSize) {
                HBaseOperations.performDelete(hTable, deleteList);
                totalDeleteSize = 0;
            }
        }

        Util.closeQuietly(rows);
        HBaseOperations.performDelete(hTable, deleteList);
    }

    @Override
    public void flush() {
        HBaseOperations.performFlush(hTable);
    }

    @Override
    public Row get(UUID uuid) {
        DataRow dataRow = new DataRow(this.tableId, uuid);
        Get get = new Get(dataRow.encode());
        Result result = HBaseOperations.performGet(hTable, get);
        if (result.isEmpty()) {
            throw new RowNotFoundException(uuid);
        }

        return Row.deserialize(result.getValue(Constants.NIC, new byte[0]));
    }

    @Override
    public Scanner tableScan() {
        DataRow startRow = new DataRow(this.tableId);
        DataRow endRow = new DataRow(this.tableId + 1);
        return createScannerForRange(startRow.encode(), endRow.encode());
    }

    @Override
    public Scanner ascendingIndexScanAt(IndexKey key) {
        IndexRow startRow = indexPrefixedForTable(key)
                .withSortOrder(SortOrder.Ascending)
                .build();

        IndexRow endRow = IndexRowBuilder
                .newBuilder(tableId, startRow.getIndexId() + 1)
                .withSortOrder(SortOrder.Ascending)
                .build();
        return createScannerForRange(startRow.encode(), endRow.encode());
    }

    @Override
    public Scanner ascendingIndexScanAfter(IndexKey key) {
        IndexRow startRow = indexPrefixedForTable(key)
                .withSortOrder(SortOrder.Ascending)
                .withUUID(Constants.FULL_UUID)
                .build();

        IndexRow endRow = IndexRowBuilder
                .newBuilder(tableId, startRow.getIndexId() + 1)
                .withSortOrder(SortOrder.Ascending)
                .build();
        return createScannerForRange(padKeyForSorting(startRow.encode()), endRow.encode());
    }

    @Override
    public Scanner descendingIndexScanAt(IndexKey key) {
        IndexRow startRow = indexPrefixedForTable(key)
                .withSortOrder(SortOrder.Descending)
                .build();

        IndexRow endRow = IndexRowBuilder
                .newBuilder(tableId, startRow.getIndexId() + 1)
                .withSortOrder(SortOrder.Descending)
                .build();
        return createScannerForRange(startRow.encode(), endRow.encode());
    }

    @Override
    public Scanner descendingIndexScanAfter(IndexKey key) {
        IndexRow startRow = indexPrefixedForTable(key)
                .withSortOrder(SortOrder.Descending)
                .withUUID(Constants.FULL_UUID)
                .build();

        IndexRow endRow = IndexRowBuilder
                .newBuilder(tableId, startRow.getIndexId() + 1)
                .withSortOrder(SortOrder.Descending)
                .build();
        return createScannerForRange(padKeyForSorting(startRow.encode()), endRow.encode());
    }

    @Override
    public Scanner indexScanExact(IndexKey key) {
        IndexRowBuilder builder = indexPrefixedForTable(key).withSortOrder(SortOrder.Ascending);
        IndexRow startRow = builder.withUUID(Constants.ZERO_UUID).build();
        IndexRow endRow = builder.withUUID(Constants.FULL_UUID).build();
        // Scan is [start, end) : add a zero to put the end key after an all 0xFF UUID
        return createScannerForRange(startRow.encode(), padKeyForSorting(endRow.encode()));
    }

    @Override
    public void close() {
        Util.closeQuietly(hTable);
    }

    private void doToIndices(Row row, IndexAction action) {
        Map<String, byte[]> records = row.getRecords();
        Map<String, Long> indexIds = this.store.getIndices(this.tableId);

        for (Map.Entry<String, IndexSchema> index : schema.getIndices().entrySet()) {
            long indexId = indexIds.get(index.getKey());
            IndexRowBuilder builder = IndexRowBuilder
                    .newBuilder(tableId, indexId)
                    .withUUID(row.getUUID())
                    .withRecords(records, getColumnTypesForSchema(schema), index.getValue().getColumns());
            action.execute(builder);
        }
    }

    private Map<String, ColumnType> getColumnTypesForSchema(TableSchema schema) {
        final ImmutableMap.Builder<String, ColumnType> result = ImmutableMap.builder();
        for (Map.Entry<String, ColumnSchema> entry : schema.getColumns().entrySet()) {
            result.put(entry.getKey(), entry.getValue().getType());
        }

        return result.build();
    }

    private Put createEmptyQualifierPut(RowKey row, byte[] serializedRow) {
        return new Put(row.encode()).add(Constants.NIC, new byte[0], serializedRow);
    }

    private Delete createEmptyQualifierDelete(RowKey row) {
        return new Delete(row.encode());
    }

    private byte[] padKeyForSorting(byte[] key) {
        return Bytes.padTail(key, 1);
    }

    private IndexRowBuilder indexPrefixedForTable(IndexKey key) {
        Map<String, Long> indices = this.store.getIndices(this.tableId);
        long indexId = indices.get(key.getIndexName());
        IndexSchema indexSchema = schema.getIndices().get(key.getIndexName());
        IndexRowBuilder indexRowBuilder = IndexRowBuilder.newBuilder(tableId, indexId);
        if (key.getKeys() == null) {
            return indexRowBuilder;
        }
        return indexRowBuilder
                .withRecords(key.getKeys(), getColumnTypesForSchema(schema), indexSchema.getColumns());
    }

    private Scanner createScannerForRange(byte[] start, byte[] end) {
        Scan scan = new Scan(start, end);
        ResultScanner scanner = HBaseOperations.getScanner(hTable, scan);
        return new HBaseScanner(scanner);
    }

    private interface IndexAction {
        public void execute(IndexRowBuilder builder);
    }
}
