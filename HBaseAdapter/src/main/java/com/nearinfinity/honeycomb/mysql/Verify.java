package com.nearinfinity.honeycomb.mysql;

import com.nearinfinity.honeycomb.mysql.gen.TableSchema;

/**
 * Checks that operations are valid.
 */
public class Verify {
    public static boolean isAutoIncColumn(String columnName, TableSchema schema)
            throws Exception {
        return schema.getColumns().get(columnName).getIsAutoincrement();
    }
}