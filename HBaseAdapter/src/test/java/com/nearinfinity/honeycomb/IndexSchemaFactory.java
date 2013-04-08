package com.nearinfinity.honeycomb;

import com.nearinfinity.honeycomb.mysql.IndexSchema;

import java.util.List;

public class IndexSchemaFactory {
    public static IndexSchema createIndexSchema(List<String> columns, boolean isUnique, String indexName) {
        return new IndexSchema(columns, isUnique, indexName);
    }

    public static IndexSchema createIndexSchema(List<String> columns, boolean isUnique) {
        return createIndexSchema(columns, isUnique, "default");
    }
}
