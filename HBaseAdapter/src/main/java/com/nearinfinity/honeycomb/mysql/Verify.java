package com.nearinfinity.honeycomb.mysql;

import com.nearinfinity.honeycomb.mysql.gen.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.gen.IndexSchema;
import com.nearinfinity.honeycomb.mysql.gen.TableSchema;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Map;

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
}