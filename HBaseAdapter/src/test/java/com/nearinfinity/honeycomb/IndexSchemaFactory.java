package com.nearinfinity.honeycomb;

import com.nearinfinity.honeycomb.mysql.IndexSchema;
import com.nearinfinity.honeycomb.mysql.gen.AvroIndexSchema;

import java.util.List;

public class IndexSchemaFactory {
    public static IndexSchema createIndexSchema(List<String> columns, boolean isUnique, String indexName) {
        AvroIndexSchema avroIndexSchema = new AvroIndexSchema(columns, isUnique);
        return new IndexSchema(avroIndexSchema, indexName);
    }

    public static IndexSchema createIndexSchema(List<String> columns, boolean isUnique) {
        return createIndexSchema(columns, isUnique, "");
    }
}
