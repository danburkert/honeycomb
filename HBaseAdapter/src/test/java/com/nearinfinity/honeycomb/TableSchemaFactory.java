package com.nearinfinity.honeycomb;

import com.nearinfinity.honeycomb.mysql.schema.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.schema.IndexSchema;
import com.nearinfinity.honeycomb.mysql.schema.TableSchema;

import java.util.Map;

public class TableSchemaFactory {
    public static TableSchema createTableSchema(Map<String, ColumnSchema> columns, Map<String, IndexSchema> indices) {
        return new TableSchema(columns, indices);
    }
}
