package com.nearinfinity.honeycomb;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.nearinfinity.honeycomb.mysql.schema.IndexSchema;

public class IndexSchemaFactory {
    private static final String DEFAULT_INDEX_NAME = "default";

    public static IndexSchema createIndexSchema(List<String> columns, boolean isUnique, String indexName) {
        return new IndexSchema(columns, isUnique, indexName);
    }

    public static IndexSchema createIndexSchema(List<String> columns, boolean isUnique) {
        return createIndexSchema(columns, isUnique, DEFAULT_INDEX_NAME);
    }

    public static IndexSchema createIndexSchema() {
        return new IndexSchema(ImmutableList.<String>of(), false, DEFAULT_INDEX_NAME);
    }
}
