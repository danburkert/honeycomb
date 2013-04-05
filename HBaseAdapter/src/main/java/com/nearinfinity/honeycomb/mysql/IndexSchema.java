package com.nearinfinity.honeycomb.mysql;

import com.nearinfinity.honeycomb.mysql.gen.AvroIndexSchema;

import java.util.List;


public class IndexSchema {
    private final AvroIndexSchema avroIndexSchema;

    public IndexSchema() {
        avroIndexSchema = new AvroIndexSchema();
    }

    public IndexSchema(List<String> columns, boolean isUnique) {
        avroIndexSchema = new AvroIndexSchema(columns, isUnique);
    }

    public List<String> getColumns() {
        return avroIndexSchema.getColumns();
    }

    public boolean getIsUnique() {
        return avroIndexSchema.getIsUnique();
    }
}
