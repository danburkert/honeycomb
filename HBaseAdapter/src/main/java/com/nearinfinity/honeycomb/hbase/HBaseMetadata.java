package com.nearinfinity.honeycomb.hbase;

import com.nearinfinity.honeycomb.mysql.gen.ColumnMetadata;
import com.nearinfinity.honeycomb.mysql.gen.TableMetadata;
import org.apache.hadoop.hbase.client.HTableInterface;

import java.io.IOException;

/**
 * Manages writing and reading table & column metadata to and from HBase.
 */
public class HBaseMetadata {
    public static TableMetadata getTableMetadata(HTableInterface hTable, String table)
            throws IOException {
        return null;
    }

    public static ColumnMetadata getColumnMetadata(HTableInterface hTable, String table, String column)
            throws IOException {
        return null;
    }

    public static void putTableMetadata(HTableInterface hTable, String table, TableMetadata metadata)
            throws IOException {
    }

    public static void putColumnMetadata(HTableInterface hTable, String table, ColumnMetadata metadata)
            throws IOException {
    }

    private static long getTableId(HTableInterface hTable, String table)
            throws IOException {
        return 0;
    }

    private static long getColumnId(HTableInterface hTable, String column)
            throws IOException {
        return 0;
    }
}