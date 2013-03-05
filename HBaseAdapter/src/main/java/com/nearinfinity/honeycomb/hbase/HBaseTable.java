package com.nearinfinity.honeycomb.hbase;

import com.nearinfinity.honeycomb.RowNotFoundException;
import com.nearinfinity.honeycomb.Scanner;
import com.nearinfinity.honeycomb.Table;
import com.nearinfinity.honeycomb.mysql.Row;
import org.apache.hadoop.hbase.client.HTableInterface;

import java.io.IOException;
import java.util.UUID;

public class HBaseTable implements Table {
    final private HTableInterface hTable;
    final private long tableId;

    public HBaseTable(HTableInterface hTable, long tableId) {
        this.hTable = hTable;
        this.tableId = tableId;
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
