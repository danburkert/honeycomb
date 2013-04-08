package com.nearinfinity.honeycomb;

import com.google.common.collect.Maps;
import com.nearinfinity.honeycomb.mysql.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.IndexSchema;
import com.nearinfinity.honeycomb.mysql.TableSchema;
import com.nearinfinity.honeycomb.mysql.gen.AvroColumnSchema;
import com.nearinfinity.honeycomb.mysql.gen.AvroIndexSchema;
import com.nearinfinity.honeycomb.mysql.gen.AvroTableSchema;

import java.util.Map;

public class TableSchemaFactory {
    public static TableSchema createTableSchema(Map<String, ColumnSchema> columns, Map<String, IndexSchema> indices) {
        Map<String, AvroColumnSchema> columnSchemaMap = Maps.newHashMap();
        for (Map.Entry<String, ColumnSchema> entry : columns.entrySet()) {
            columnSchemaMap.put(entry.getKey(), entry.getValue().getAvroValue());
        }

        Map<String, AvroIndexSchema> indexSchemaMap = Maps.newHashMap();
        for (Map.Entry<String, IndexSchema> entry : indices.entrySet()) {
            indexSchemaMap.put(entry.getKey(), entry.getValue().getAvroValue());
        }

        AvroTableSchema avroTableSchema = new AvroTableSchema(columnSchemaMap, indexSchemaMap);
        return new TableSchema(avroTableSchema);
    }
}
