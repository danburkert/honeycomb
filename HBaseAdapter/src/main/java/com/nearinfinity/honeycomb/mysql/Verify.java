package com.nearinfinity.honeycomb.mysql;

import com.nearinfinity.honeycomb.mysql.gen.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.gen.TableSchema;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Checks that operations are valid.
 */
public class Verify {
    public static boolean hasAutoIncrementColumn(TableSchema schema) {
        Map<String, ColumnSchema> columns = schema.getColumns();
        for (ColumnSchema column : columns.values()) {
            if (column.getIsAutoIncrement()) {
                return true;
            }
        }
        return false;
    }

    public static void isNotNullOrEmpty(String value, String... message) {
        checkNotNull(value, message);
        checkArgument(!value.isEmpty(), message);
    }
}