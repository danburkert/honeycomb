package com.nearinfinity.honeycomb.mysql;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.nearinfinity.honeycomb.RuntimeIOException;
import com.nearinfinity.honeycomb.Scanner;
import com.nearinfinity.honeycomb.Store;
import com.nearinfinity.honeycomb.Table;
import com.nearinfinity.honeycomb.mysql.gen.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.gen.IndexSchema;
import com.nearinfinity.honeycomb.mysql.gen.TableSchema;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Checks that operations are valid.
 */
public class Verify {
    private static final Logger logger = Logger.getLogger(Verify.class);

    public static boolean hasAutoIncrementColumn(TableSchema schema) {
        Map<String, ColumnSchema> columns = schema.getColumns();
        for (ColumnSchema column : columns.values()) {
            if (column.getIsAutoIncrement()) {
                return true;
            }
        }
        return false;
    }

    public static void isValidTableId(final long tableId, String... message) {
        checkArgument(tableId >= 0, "Table id must be greater than or equal to zero. " + Arrays.toString(message));
    }

    public static void isNotNullOrEmpty(String value, String... message) {
        checkNotNull(value, message);
        checkArgument(!value.isEmpty(), message);
    }

    /**
     * Verifies that the table schema and its constituent parts are valid. (Warning: expensive check).
     * @param schema Table schema to validate.
     */
    public static void isValidTableSchema(TableSchema schema) {
        checkNotNull(schema);
        isValidIndexSchema(schema.getIndices(), schema.getColumns());
    }

    private static void isValidIndexSchema(Map<String, IndexSchema> indices,
                                          Map<String, ColumnSchema> columns) {
        for (IndexSchema index : indices.values()) {
            for (String column : index.getColumns()) {
                if (!columns.containsKey(column)) {
                    throw new IllegalArgumentException("Only columns in the table may be indexed.");
                }
            }
        }
    }

    /**
     * Checks if a table contains duplicate rows over the columns provided.
     * TODO: extend to use existing indices on the table (if helpful).
     * @param columns columns to check for duplicates
     * @param size estimated number of rows in table
     */
    public static boolean tableContainsDuplicates(Store store, String tableName,
                                                  List<String> columns, int size) {
        BloomFilter<byte[]> filter =
                BloomFilter.create(Funnels.byteArrayFunnel(), size, 0.01);
        Map<String, byte[]> records;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] recordBytes;
        Table table = store.openTable(tableName);
        Scanner tableScan = table.tableScan();
        Row row;
        while (tableScan.hasNext()) {
            row = tableScan.next();
            baos.reset();
            records = row.getRecords();
            for (String column : columns) {
                try {
                    baos.write(records.get(column));
                } catch (IOException e) {
                    logger.error("IOException thrown while checking table for" +
                            " duplicate rows.", e);
                    throw new RuntimeIOException(e);
                }
            }
            recordBytes = baos.toByteArray();
            if (filter.mightContain(recordBytes)) {
                Table containsTable = store.openTable(tableName);
                if (tableContainsRecord(containsTable,
                        Util.selectKeys(records, columns), row.getUUID())) {
                    containsTable.close();
                    tableScan.close();
                    table.close();
                    return true;
                }
                containsTable.close();
            }
            filter.put(recordBytes);
        }
        tableScan.close();
        table.close();
        return false;
    }

    /**
     * Check if the table contains a row with the same values as the record
     * passed in.  Skips rows if the UUID matches the argument
     */
    public static boolean tableContainsRecord(Table table,
                                              Map<String, byte[]> records,
                                              UUID skip) {
        Set<Map.Entry<String, byte[]>> recordSet =
                ImmutableSet.copyOf(records.entrySet());
        Set<Map.Entry<String, byte[]>> rowSet;
        Scanner tableScan = table.tableScan();
        Row row;
        while (table.tableScan().hasNext()) {
            row = tableScan.next();
            rowSet = row.getRecords().entrySet();
            if (Sets.difference(recordSet, rowSet).size() == 0
                    && !row.getUUID().equals(skip)) {
                tableScan.close();
                return true;
            }
        }
        return false;
    }
}