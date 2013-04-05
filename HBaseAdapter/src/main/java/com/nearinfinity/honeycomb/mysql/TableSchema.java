package com.nearinfinity.honeycomb.mysql;

import com.google.common.collect.Maps;
import com.nearinfinity.honeycomb.mysql.gen.AvroColumnSchema;
import com.nearinfinity.honeycomb.mysql.gen.AvroIndexSchema;
import com.nearinfinity.honeycomb.mysql.gen.AvroTableSchema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.util.Collection;
import java.util.Map;

public class TableSchema {
    private static final DatumWriter<AvroTableSchema> writer =
            new SpecificDatumWriter<AvroTableSchema>(AvroTableSchema.class);
    private static final DatumReader<AvroTableSchema> reader =
            new SpecificDatumReader<AvroTableSchema>(AvroTableSchema.class);
    private final AvroTableSchema avroTableSchema;
    private final Map<String, ColumnSchema> columns;
    private final Map<String, IndexSchema> indices;

    public TableSchema(Map<String, ColumnSchema> columns, Map<String, IndexSchema> indices) {
        this.columns = columns;
        this.indices = indices;
        this.avroTableSchema = initializeAvroTableSchema(columns, indices);
    }

    private TableSchema(AvroTableSchema avroTableSchema) {
        this.avroTableSchema = avroTableSchema;

        this.columns = Maps.newHashMap();
        for (Map.Entry<String, AvroColumnSchema> entry : avroTableSchema.getColumns().entrySet()) {
            this.columns.put(entry.getKey(), new ColumnSchema(entry.getValue()));
        }

        this.indices = Maps.newHashMap();
        for (Map.Entry<String, AvroIndexSchema> entry : avroTableSchema.getIndices().entrySet()) {
            this.indices.put(entry.getKey(), new IndexSchema(entry.getValue()));
        }
    }

    public static TableSchema deserialize(byte[] serializedTableSchema) {
        return new TableSchema(Util.deserializeAvroObject(serializedTableSchema, reader));
    }

    /**
     * Produce a copy of the current schema such that the two schemas are independent
     * each other.
     *
     * @return New independent table schema
     */
    public TableSchema schemaCopy() {
        return new TableSchema(AvroTableSchema.newBuilder(avroTableSchema).build());
    }

    public byte[] serialize() {
        return Util.serializeAvroObject(avroTableSchema, writer);
    }

    public Map<String, ColumnSchema> getColumns() {
        return columns;
    }

    public Map<String, IndexSchema> getIndices() {
        return indices;
    }

    public void addIndices(Map<String, IndexSchema> indices) {
        getIndices().putAll(indices);
        for (Map.Entry<String, IndexSchema> entry : indices.entrySet()) {
            avroTableSchema.getIndices().put(entry.getKey(), entry.getValue().getAvroValue());
        }
    }

    public void removeIndex(String indexName) {
        getIndices().remove(indexName);
        avroTableSchema.getIndices().remove(indexName);
    }

    public boolean hasIndices() {
        return !getIndices().isEmpty();
    }

    public Collection<Map.Entry<String, IndexSchema>> getIndexSchemaEntries() {
        return getIndices().entrySet();
    }

    public IndexSchema getIndexSchemaForName(String indexName) {
        return getIndices().get(indexName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TableSchema that = (TableSchema) o;

        if (avroTableSchema != null ? !avroTableSchema.equals(that.avroTableSchema) : that.avroTableSchema != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        return avroTableSchema != null ? avroTableSchema.hashCode() : 0;
    }

    private AvroTableSchema initializeAvroTableSchema(Map<String, ColumnSchema> columns, Map<String, IndexSchema> indices) {
        Map<String, AvroColumnSchema> columnSchemaMap = Maps.newHashMap();
        Map<String, AvroIndexSchema> indexSchemaMap = Maps.newHashMap();
        for (Map.Entry<String, ColumnSchema> entry : columns.entrySet()) {
            columnSchemaMap.put(entry.getKey(), entry.getValue().getAvroValue());
        }
        for (Map.Entry<String, IndexSchema> entry : indices.entrySet()) {
            indexSchemaMap.put(entry.getKey(), entry.getValue().getAvroValue());
        }

        return new AvroTableSchema(columnSchemaMap, indexSchemaMap);
    }
}
