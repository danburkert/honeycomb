package com.nearinfinity.honeycomb.hbase;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.nearinfinity.honeycomb.RowNotFoundException;
import com.nearinfinity.honeycomb.Scanner;
import com.nearinfinity.honeycomb.Table;
import com.nearinfinity.honeycomb.TableNotFoundException;
import com.nearinfinity.honeycomb.hbase.rowkey.AscIndexRow;
import com.nearinfinity.honeycomb.hbase.rowkey.DataRow;
import com.nearinfinity.honeycomb.hbase.rowkey.DescIndexRow;
import com.nearinfinity.honeycomb.hbaseclient.Constants;
import com.nearinfinity.honeycomb.mysql.Row;
import com.nearinfinity.honeycomb.mysql.gen.IndexSchema;
import com.nearinfinity.honeycomb.mysql.gen.TableSchema;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.LinkedList;
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
    public void insert(Row row) throws IOException {
        final byte[] serializeRow = row.serialize();
        final UUID uuid = row.getUUID();
        hTable.put(createEmptyQualifierPut(new DataRow(this.tableId, uuid), serializeRow));
        doToIndices(row, new IndexAction() {
            @Override
            public void execute(long indexId, List<byte[]> sortedRecords) throws IOException {
                hTable.put(createEmptyQualifierPut(new DescIndexRow(tableId, indexId, sortedRecords, uuid), serializeRow));
                hTable.put(createEmptyQualifierPut(new AscIndexRow(tableId, indexId, sortedRecords, uuid), serializeRow));
            }
        });
    }

    @Override
    public void update(Row row) throws IOException, RowNotFoundException {
        delete(row.getUUID());
        insert(row);
    }

    @Override
    public void delete(final UUID uuid) throws IOException, RowNotFoundException {
        Row row = get(uuid);
        final List<Delete> deleteList = Lists.newLinkedList();
        deleteList.add(new Delete(new DataRow(this.tableId, uuid).encode()));
        doToIndices(row, new IndexAction() {
            @Override
            public void execute(long indexId, List<byte[]> sortedRecords) throws IOException {
                deleteList.add(createEmptyQualifierDelete(new DescIndexRow(tableId, indexId, sortedRecords, uuid)));
                deleteList.add(createEmptyQualifierDelete(new AscIndexRow(tableId, indexId, sortedRecords, uuid)));
            }
        });

        hTable.delete(deleteList);
    }

    @Override
    public void deleteAllRows() throws IOException {
        long totalDeleteSize = 0, writeBufferSize = 50000; // TODO: retrieve write buffer size from configuration
        final List<Delete> deleteList = Lists.newLinkedList();
        Scanner rows = this.tableScan();
        for (Row row : rows) {
            final UUID uuid = row.getUUID();
            deleteList.add(new Delete(new DataRow(this.tableId, uuid).encode()));
            doToIndices(row, new IndexAction() {
                @Override
                public void execute(long indexId, List<byte[]> sortedRecords) throws IOException {
                    deleteList.add(createEmptyQualifierDelete(new DescIndexRow(tableId, indexId, sortedRecords, uuid)));
                    deleteList.add(createEmptyQualifierDelete(new AscIndexRow(tableId, indexId, sortedRecords, uuid)));
                }
            });

            totalDeleteSize += deleteList.size() * row.serialize().length;
            if (totalDeleteSize > writeBufferSize) {
                hTable.delete(deleteList);
                totalDeleteSize = 0;
            }
        }

        rows.close();
        hTable.delete(deleteList);
    }

    @Override
    public void flush() throws IOException {
        this.hTable.flushCommits();
    }

    @Override
    public Row get(UUID uuid) throws RowNotFoundException, IOException {
        DataRow dataRow = new DataRow(this.tableId, uuid);
        Get get = new Get(dataRow.encode());
        Result result = this.hTable.get(get);
        if (result.isEmpty()) {
            throw new RowNotFoundException(uuid);
        }

        return Row.deserialize(result.getValue(Constants.NIC, new byte[0]));
    }

    @Override
    public Scanner tableScan() throws IOException {
        DataRow startRow = new DataRow(this.tableId);
        DataRow endRow = new DataRow(this.tableId + 1);
        Scan scan = new Scan(startRow.encode(), endRow.encode());
        final ResultScanner scanner = this.hTable.getScanner(scan);
        return new HBaseScanner(scanner);
    }

    @Override
    public Scanner ascendingIndexScanAt() {
        return null;
    }

    @Override
    public Scanner ascendingIndexScanAfter() {
        return null;
    }

    @Override
    public Scanner descendingIndexScanAt() {
        return null;
    }

    @Override
    public Scanner descendingIndexScanAfter() {
        return null;
    }

    @Override
    public Scanner indexScanExact() {
        return null;
    }

    @Override
    public void close() throws IOException {
        this.hTable.close();
    }

    private void doToIndices(Row row, IndexAction action) throws IOException {
        Map<String, byte[]> records = row.getRecords();
        Map<String, Long> indexIds;
        try {
            indexIds = this.store.getIndices(this.tableId);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }

        for (Map.Entry<String, IndexSchema> index : schema.getIndices().entrySet()) {
            long indexId = indexIds.get(index.getKey());
            List<byte[]> sortedRecords = getValuesInColumnOrder(records, index.getValue().getColumns());
            action.execute(indexId, sortedRecords);
        }
    }

    private List<byte[]> getValuesInColumnOrder(Map<String, byte[]> records, List<String> columns) {
        List<byte[]> sortedRecords = new LinkedList<byte[]>();
        for (String column : columns) {
            sortedRecords.add(records.get(column));
        }
        return sortedRecords;
    }

    private Put createEmptyQualifierPut(RowKey row, byte[] serializedRow) {
        return new Put(row.encode()).add(Constants.NIC, new byte[0], serializedRow);
    }

    private Delete createEmptyQualifierDelete(RowKey row) {
        return new Delete(row.encode());
    }

    private interface IndexAction {
        public void execute(long indexId, List<byte[]> sortedRecords) throws IOException;
    }
}
