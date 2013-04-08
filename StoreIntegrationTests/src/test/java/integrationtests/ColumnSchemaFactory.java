package integrationtests;

import com.nearinfinity.honeycomb.mysql.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.gen.AvroColumnSchema;
import com.nearinfinity.honeycomb.mysql.gen.ColumnType;

public class ColumnSchemaFactory {

    public static ColumnSchema createColumnSchema(ColumnType type, boolean isNullable, boolean isAutoIncrement, int maxLength, int scale, int precision) {
        return new ColumnSchema(type, isNullable, isAutoIncrement, maxLength, scale, precision, "default");
    }

    public static ColumnSchema createColumnSchema(ColumnType type, boolean isNullable, boolean isAutoIncrement, int maxLength, int scale, int precision, String columnName) {
        return new ColumnSchema(type, isNullable, isAutoIncrement, maxLength, scale, precision, columnName);
    }
}
