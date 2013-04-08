package com.nearinfinity.honeycomb;

import com.google.common.collect.ImmutableList;
import com.nearinfinity.honeycomb.mysql.IndexSchema;
import com.nearinfinity.honeycomb.mysql.gen.AvroIndexSchema;

import java.util.List;

public class IndexSchemaFactory {
    public static IndexSchema createIndexSchema(List<String> columns, boolean isUnique, String indexName) {
        AvroIndexSchema avroIndexSchema = new AvroIndexSchema(columns, isUnique);
        return new IndexSchema(indexName, avroIndexSchema);
    }

    public static IndexSchema createIndexSchema(List<String> columns, boolean isUnique) {
        return createIndexSchema(columns, isUnique, "default");
    }

    public static IndexSchema createIndexSchema() {
        AvroIndexSchema avroIndexSchema = new AvroIndexSchema(ImmutableList.<String>of(), false);
        return new IndexSchema("default", avroIndexSchema);
    }
}
