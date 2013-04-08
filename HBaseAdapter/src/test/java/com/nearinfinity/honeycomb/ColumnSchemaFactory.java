package com.nearinfinity.honeycomb;

import com.nearinfinity.honeycomb.mysql.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.gen.AvroColumnSchema;
import com.nearinfinity.honeycomb.mysql.gen.ColumnType;

public class ColumnSchemaFactory {

    public static ColumnSchema createColumnSchema(ColumnType type, boolean isNullable, boolean isAutoIncrement, int maxLength, int scale, int precision) {
        AvroColumnSchema avroColumnSchema = new AvroColumnSchema(type, isNullable, isAutoIncrement, maxLength, scale, precision);
        return new ColumnSchema("default", avroColumnSchema);
    }

    public static ColumnSchema createColumnSchema(ColumnType type, boolean isNullable, boolean isAutoIncrement, int maxLength, int scale, int precision, String columnName) {
        AvroColumnSchema avroColumnSchema = new AvroColumnSchema(type, isNullable, isAutoIncrement, maxLength, scale, precision);
        return new ColumnSchema(columnName, avroColumnSchema);
    }

    public static ColumnSchema createColumnSchema() {
        AvroColumnSchema avroColumnSchema = new AvroColumnSchema(ColumnType.LONG, false, false, 0, 0, 0);
        return new ColumnSchema("default", avroColumnSchema);
    }
}
