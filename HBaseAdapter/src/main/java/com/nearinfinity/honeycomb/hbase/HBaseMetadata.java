package com.nearinfinity.honeycomb.hbase;

import com.nearinfinity.honeycomb.mysql.gen.ColumnMetadata;
import com.nearinfinity.honeycomb.mysql.gen.TableMetadata;

import java.io.IOException;

/**
 * Manages writing and reading table & column metadata to and from HBase.
 */
public class HBaseMetadata {
    public TableMetadata getTableMetadata(String table) throws IOException {
        return null;
    }
    public ColumnMetadata getColumnMetadata(String table, String column)
            throws IOException {
        return null;
    }
    public void putTableMetadata(String table, TableMetadata metadata)
            throws IOException {
    }
    public void putColumnMetadata(String table, ColumnMetadata metadata)
            throws IOException {
    }
}