package com.nearinfinity.honeycomb.hbase;

import static com.google.common.base.Preconditions.checkNotNull;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.name.Named;
import com.nearinfinity.honeycomb.exceptions.RowNotFoundException;
import com.nearinfinity.honeycomb.Scanner;
import com.nearinfinity.honeycomb.Table;
import com.nearinfinity.honeycomb.config.ConfigConstants;
import com.nearinfinity.honeycomb.hbase.rowkey.DataRow;
import com.nearinfinity.honeycomb.hbase.rowkey.IndexRow;
import com.nearinfinity.honeycomb.hbase.rowkey.IndexRowBuilder;
import com.nearinfinity.honeycomb.hbase.rowkey.SortOrder;
import com.nearinfinity.honeycomb.config.Constants;
import com.nearinfinity.honeycomb.mysql.IndexKey;
import com.nearinfinity.honeycomb.mysql.Row;
import com.nearinfinity.honeycomb.mysql.Util;
import com.nearinfinity.honeycomb.mysql.Verify;
import com.nearinfinity.honeycomb.mysql.gen.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.gen.ColumnType;
import com.nearinfinity.honeycomb.mysql.gen.IndexSchema;
import com.nearinfinity.honeycomb.mysql.gen.QueryType;
import com.nearinfinity.honeycomb.mysql.gen.TableSchema;

public class HBaseTable implements Table {
    private static final Logger logger = Logger.getLogger(HBaseTable.class);

    private final HTableInterface hTable;
    private final HBaseStore store;
    private final long tableId;
    private long writeBufferSize = ConfigConstants.DEFAULT_WRITE_BUFFER_SIZE;

    @Inject
    public HBaseTable(HTableInterface hTable, HBaseStore store, @Assisted Long tableId) {
        Verify.isValidTableId(tableId);
        this.hTable = checkNotNull(hTable);
        this.store = checkNotNull(store);
        this.tableId = tableId;
    }

    @Inject
    public void setWriterBufferSize(final @Named(ConfigConstants.PROP_WRITE_BUFFER_SIZE) Long bufferSize) {
        writeBufferSize = bufferSize;
    }

    @Override
    public void insert(Row row) {
        checkNotNull(row);
        final byte[] serializeRow = row.serialize();
        final UUID uuid = row.getUUID();

        HBaseOperations.performPut(hTable, createEmptyQualifierPut(new DataRow(tableId, uuid), serializeRow));
        doToIndices(row, getTableIndices(tableId), new IndexAction() {
            @Override
            public void execute(IndexRowBuilder builder) {
                HBaseOperations.performPut(hTable, createEmptyQualifierPut(builder.withSortOrder(SortOrder.Ascending).build(), serializeRow));
                HBaseOperations.performPut(hTable, createEmptyQualifierPut(builder.withSortOrder(SortOrder.Descending).build(), serializeRow));
            }
        });
    }

    @Override
    public void insertTableIndex(String indexName, IndexSchema indexSchema) {
        Verify.isNotNullOrEmpty(indexName, "The index name is invalid");
        checkNotNull(indexSchema, "The index schema is invalid");

        final Scanner dataRows = tableScan();

        if( dataRows.hasNext() ) {
            logger.debug("Inserting indices for named index: " + indexName);
        } else {
            logger.info("There are no data rows available to index");
        }

        while (dataRows.hasNext()) {
            final Row dataRow = dataRows.next();
            doToIndices(dataRow, ImmutableMap.<String, IndexSchema>of(indexName, indexSchema), new IndexAction() {
                @Override
                public void execute(IndexRowBuilder builder) {
                    HBaseOperations.performPut(hTable, createEmptyQualifierPut(builder.withSortOrder(SortOrder.Ascending).build(), dataRow.serialize()));
                    HBaseOperations.performPut(hTable, createEmptyQualifierPut(builder.withSortOrder(SortOrder.Descending).build(), dataRow.serialize()));
                }
            });
        }

        Util.closeQuietly(dataRows);
    }

    @Override
    public void update(Row row) {
        checkNotNull(row);
        delete(row.getUUID());
        insert(row);
    }

    @Override
    public void delete(final UUID uuid) {
        checkNotNull(uuid);
        Row row = get(uuid);
        final List<Delete> deleteList = Lists.newLinkedList();
        deleteList.add(new Delete(new DataRow(tableId, uuid).encode()));
        doToIndices(row, getTableIndices(tableId), new IndexAction() {
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
        long totalDeleteSize = 0;
        final List<Delete> deleteList = Lists.newLinkedList();
        final Scanner rows = tableScan();

        while (rows.hasNext()) {
            final Row row = rows.next();
            final UUID uuid = row.getUUID();
            deleteList.add(new Delete(new DataRow(tableId, uuid).encode()));
            doToIndices(row, getTableIndices(tableId), new IndexAction() {
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
        DataRow dataRow = new DataRow(tableId, uuid);
        Get get = new Get(dataRow.encode());
        Result result = HBaseOperations.performGet(hTable, get);
        if (result.isEmpty()) {
            throw new RowNotFoundException(uuid);
        }

        return Row.deserialize(result.getValue(Constants.DEFAULT_COLUMN_FAMILY, new byte[0]));
    }

    @Override
    public Scanner tableScan() {
        DataRow startRow = new DataRow(tableId);
        DataRow endRow = new DataRow(tableId + 1);
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

    private void doToIndices(final Row row, final Map<String, IndexSchema> indices, final IndexAction action) {
        final Map<String, ByteBuffer> records = row.getRecords();
        final Map<String, Long> indexIds = store.getIndices(tableId);
        final TableSchema schema = store.getSchema(tableId);

        for (Map.Entry<String, IndexSchema> index : indices.entrySet()) {
            long indexId = indexIds.get(index.getKey());

            IndexRowBuilder builder = IndexRowBuilder
                    .newBuilder(tableId, indexId)
                    .withUUID(row.getUUID())
                    .withRecords(records, getColumnTypesForSchema(schema), index.getValue().getColumns());
            action.execute(builder);
        }
    }

    private static Map<String, ColumnType> getColumnTypesForSchema(TableSchema schema) {
        final ImmutableMap.Builder<String, ColumnType> result = ImmutableMap.builder();
        for (Map.Entry<String, ColumnSchema> entry : schema.getColumns().entrySet()) {
            result.put(entry.getKey(), entry.getValue().getType());
        }

        return result.build();
    }

    private static Put createEmptyQualifierPut(RowKey row, byte[] serializedRow) {
        return new Put(row.encode()).add(Constants.DEFAULT_COLUMN_FAMILY, new byte[0], serializedRow);
    }

    private static Delete createEmptyQualifierDelete(RowKey row) {
        return new Delete(row.encode());
    }

    private static byte[] padKeyForSorting(byte[] key) {
        return Bytes.padTail(key, 1);
    }

    private IndexRowBuilder indexPrefixedForTable(final IndexKey key) {
        final Map<String, Long> indices = store.getIndices(tableId);
        final TableSchema schema = store.getSchema(tableId);
        final long indexId = indices.get(key.getIndexName());
        final IndexSchema indexSchema = schema.getIndices().get(key.getIndexName());
        final IndexRowBuilder indexRowBuilder = IndexRowBuilder.newBuilder(tableId, indexId);

        if( key.getQueryType() == QueryType.INDEX_LAST || key.getQueryType() == QueryType.INDEX_FIRST ) {
            return indexRowBuilder;
        }

        return indexRowBuilder.withRecords(key.getKeys(), getColumnTypesForSchema(schema), indexSchema.getColumns());
    }

    private Scanner createScannerForRange(byte[] start, byte[] end) {
        Scan scan = new Scan(start, end);
        ResultScanner scanner = HBaseOperations.getScanner(hTable, scan);
        return new HBaseScanner(scanner);
    }

    /**
     * Fetches the index mapping information for the table with the specified table id
     * @param tableId The id of the table to consult
     * @return A mapping of index data for the specified table
     */
    private Map<String, IndexSchema> getTableIndices(final long tableId) {
        Map<String, IndexSchema> indexInfo = Maps.newHashMap();
        final TableSchema schema = store.getSchema(tableId);

        if( schema != null ) {
            final Map<String, IndexSchema> tableIndices = schema.getIndices();

            if( tableIndices != null ) {
                indexInfo = tableIndices;
            }
        }

        return indexInfo;
    }

    private interface IndexAction {
        public void execute(IndexRowBuilder builder);
    }
}
