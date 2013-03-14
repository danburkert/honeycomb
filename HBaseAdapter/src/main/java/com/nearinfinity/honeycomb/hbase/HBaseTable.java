package com.nearinfinity.honeycomb.hbase;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.nearinfinity.honeycomb.RowNotFoundException;
import com.nearinfinity.honeycomb.Scanner;
import com.nearinfinity.honeycomb.Table;
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
    private final String tableName;
    private final long tableId;

    @Inject
    public HBaseTable(HTableInterface hTable, HBaseStore store, @Assisted String tableName) throws Exception {
        this.hTable = hTable;
        this.store = store;
        this.tableName = tableName;
        this.tableId = store.getTableId(tableName);
    }

    @Override
    public void insert(Row row) {
        try {
            byte[] serializeRow = row.serialize();
            UUID uuid = row.getUUID();
            Map<String, Long> indexIds = this.store.getIndices(this.tableName);
            TableSchema schema = this.store.getSchema(this.tableName);
            Map<String, byte[]> records = row.getRecords();
            hTable.put(createEmptyQualifierPut(new DataRow(this.tableId, uuid), serializeRow));

            for (Map.Entry<String, IndexSchema> index : schema.getIndices().entrySet()) {
                long indexId = indexIds.get(index.getKey());
                List<byte[]> sortedRecords = getValuesInColumnOrder(records, index.getValue().getColumns());
                hTable.put(createEmptyQualifierPut(new DescIndexRow(this.tableId, indexId, sortedRecords, uuid), serializeRow));
                hTable.put(createEmptyQualifierPut(new AscIndexRow(this.tableId, indexId, sortedRecords, uuid), serializeRow));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void update(Row row) throws IOException, RowNotFoundException {
    }

    @Override
    public void delete(UUID uuid) throws IOException, RowNotFoundException {
    }

    @Override
    public void deleteAllRows() throws IOException {
        try {
            long totalDeleteSize = 0, writeBufferSize = 50000; // TODO: retrieve write buffer size from configuration
            TableSchema schema = this.store.getSchema(this.tableName);
            Map<String, Long> indexIds = this.store.getIndices(this.tableName);
            DataRow startRow = new DataRow(this.tableId, Constants.ZERO_UUID);
            DataRow endRow = new DataRow(this.tableId, Constants.FULL_UUID);
            Scan scan = new Scan(startRow.encode(), endRow.encode());
            ResultScanner scanner = hTable.getScanner(scan);
            List<Delete> deleteList = Lists.newLinkedList();
            for (Result result : scanner) {
                Row row = Row.deserialize(result.getValue(Constants.NIC, new byte[0]));
                UUID uuid = row.getUUID();
                deleteList.add(new Delete(new DataRow(this.tableId, uuid).encode()));
                Map<String, byte[]> records = row.getRecords();
                for (Map.Entry<String, IndexSchema> index : schema.getIndices().entrySet()) {
                    long indexId = indexIds.get(index.getKey());
                    List<byte[]> sortedRecords = getValuesInColumnOrder(records, index.getValue().getColumns());
                    deleteList.add(createEmptyQualifierDelete(new DescIndexRow(this.tableId, indexId, sortedRecords, uuid)));
                    deleteList.add(createEmptyQualifierDelete(new AscIndexRow(this.tableId, indexId, sortedRecords, uuid)));
                }
                totalDeleteSize += deleteList.size() * result.getWritableSize();
                if (totalDeleteSize > writeBufferSize) {
                    hTable.delete(deleteList);
                    totalDeleteSize = 0;
                }
            }

            hTable.delete(deleteList);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void flush() throws IOException {
        this.hTable.flushCommits();
    }

    @Override
    public Row get(UUID uuid) {
        DataRow dataRow = new DataRow(this.tableId, uuid);
        Get get = new Get(dataRow.encode());
        try {
            Result result = this.hTable.get(get);
            return Row.deserialize(result.getValue(Constants.NIC, new byte[0]));
        } catch (IOException e) {
            e.printStackTrace(); //TODO: Implement error handling
            throw new RuntimeException(e);
        }
    }

    @Override
    public Scanner tableScan() {
        return null;
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
}
