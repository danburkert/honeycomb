package com.nearinfinity.honeycomb.hbase;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.nearinfinity.honeycomb.RowNotFoundException;
import com.nearinfinity.honeycomb.Scanner;
import com.nearinfinity.honeycomb.Table;
import com.nearinfinity.honeycomb.mysql.Row;
import org.apache.hadoop.hbase.client.HTableInterface;

import java.io.IOException;
import java.util.UUID;

public class HBaseTable implements Table {
    private final HTableInterface hTable;
    private final long tableId;

    @Inject
    public HBaseTable(HTableInterface hTable, HBaseStore store, @Assisted String tableName) throws Exception {
        this.hTable = hTable;
        this.tableId = store.getTableId(tableName);
    }

    @Override
    public void insert(Row row) {
    }

    @Override
    public void update(Row row) throws IOException, RowNotFoundException {
    }

    @Override
    public void delete(UUID uuid) throws IOException, RowNotFoundException {
    }

    @Override
    public void flush() throws IOException {
    }

    @Override
    public Row get(UUID uuid) {
        return null;
    }

    @Override
    public Scanner tableScan() {
        return null;
    }

    @Override
    public Scanner AscIndexScanAt() {
        return null;
    }

    @Override
    public Scanner AscIndexScanAfter() {
        return null;
    }

    @Override
    public Scanner DescIndexScanAt() {
        return null;
    }

    @Override
    public Scanner DescIndexScanAfter() {
        return null;
    }

    @Override
    public Scanner indexScanExact() {
        return null;
    }

    @Override
    public void close() throws IOException {
    }
}
