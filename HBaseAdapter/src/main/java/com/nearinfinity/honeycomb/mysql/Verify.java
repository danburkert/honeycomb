package com.nearinfinity.honeycomb.mysql;

import com.nearinfinity.honeycomb.mysql.gen.TableSchema;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Checks that operations are valid.
 */
public class Verify {
    public static boolean isAutoIncColumn(String columnName, TableSchema schema)
            throws Exception {
        return schema.getColumns().get(columnName).getIsAutoIncrement();
    }

    public static void isNotNullOrEmpty(String value, String... message) {
        checkNotNull(value, message);
        checkArgument(!value.isEmpty(), message);
    }
}