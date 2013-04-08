package com.nearinfinity.honeycomb;

import com.nearinfinity.honeycomb.mysql.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.gen.ColumnType;

public class ColumnSchemaFactory {

    public static ColumnSchema createColumnSchema(ColumnType type, boolean isNullable, boolean isAutoIncrement, int maxLength, int scale, int precision) {
        return new ColumnSchema(type, isNullable, isAutoIncrement, maxLength, scale, precision, "default");
    }

    public static ColumnSchema createColumnSchema(ColumnType type, boolean isNullable, boolean isAutoIncrement, int maxLength, int scale, int precision, String columnName) {
        return new ColumnSchema(type, isNullable, isAutoIncrement, maxLength, scale, precision, columnName);
    }

    public static ColumnSchema createColumnSchema() {
        return new ColumnSchema(ColumnType.LONG, false, false, 0, 0, 0, "default");
    }

    public static ColumnSchema createColumnSchema(long columnId) {
        return new ColumnSchema(ColumnType.LONG, false, false, 0, 0, 0, "default" + columnId);
    }
}
