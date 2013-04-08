package com.nearinfinity.honeycomb;

import com.google.common.collect.ImmutableList;
import com.nearinfinity.honeycomb.mysql.gen.AvroIndexSchema;
import com.nearinfinity.honeycomb.mysql.schema.IndexSchema;

import java.util.List;

public class IndexSchemaFactory {
    public static IndexSchema createIndexSchema(List<String> columns, boolean isUnique, String indexName) {
        return new IndexSchema(columns, isUnique, indexName);
    }

    public static IndexSchema createIndexSchema(List<String> columns, boolean isUnique) {
        return createIndexSchema(columns, isUnique, "default");
    }

    public static IndexSchema createIndexSchema() {
        AvroIndexSchema avroIndexSchema = new AvroIndexSchema(ImmutableList.<String>of(), false);
        return new IndexSchema("default", avroIndexSchema);
    }
}
