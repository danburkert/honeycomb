package integrationtests;

import com.nearinfinity.honeycomb.mysql.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.IndexSchema;
import com.nearinfinity.honeycomb.mysql.TableSchema;

import java.util.Map;

public class TableSchemaFactory {
    public static TableSchema createTableSchema(Map<String, ColumnSchema> columns, Map<String, IndexSchema> indices) {
        return new TableSchema(columns, indices);
    }
}
