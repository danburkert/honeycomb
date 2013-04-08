package com.nearinfinity.honeycomb.mysql;

import com.google.common.collect.Lists;
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
    private final Collection<ColumnSchema> columns;
    private final Collection<IndexSchema> indices;

    public TableSchema(AvroTableSchema avroTableSchema) {
        this.avroTableSchema = avroTableSchema;

        this.columns = Lists.newArrayList();
        for (Map.Entry<String, AvroColumnSchema> entry : avroTableSchema.getColumns().entrySet()) {
            this.columns.add(new ColumnSchema(entry.getValue(), entry.getKey()));
        }

        this.indices = Lists.newArrayList();
        for (Map.Entry<String, AvroIndexSchema> entry : avroTableSchema.getIndices().entrySet()) {
            this.indices.add(new IndexSchema(entry.getValue(), entry.getKey()));
        }
    }

    public static TableSchema deserialize(byte[] serializedTableSchema) {
        return new TableSchema(Util.deserializeAvroObject(serializedTableSchema, reader));
    }

    /**
     * Produce a copy of the current schema such that the two schemas are independent
     * each other. A change to the copy doesn't affect the original.
     *
     * @return New independent table schema
     */
    public TableSchema schemaCopy() {
        return new TableSchema(AvroTableSchema.newBuilder(avroTableSchema).build());
    }

    public byte[] serialize() {
        return Util.serializeAvroObject(avroTableSchema, writer);
    }

    public Collection<ColumnSchema> getColumns() {
        return columns;
    }

    public Map<String, ColumnSchema> getColumnsMap() {
        Map<String, ColumnSchema> map = Maps.newHashMap();
        for (ColumnSchema columnSchema : columns) {
            map.put(columnSchema.getColumnName(), columnSchema);
        }

        return map;
    }

    public Collection<IndexSchema> getIndices() {
        return indices;
    }

    public void addIndices(Collection<IndexSchema> indices) {
        for (IndexSchema entry : indices) {
            getIndices().add(entry);
            avroTableSchema.getIndices().put(entry.getIndexName(), entry.getAvroValue());
        }
    }

    public void removeIndex(String indexName) {
        IndexSchema schema = getIndexSchemaForName(indexName);
        getIndices().remove(schema);
        avroTableSchema.getIndices().remove(indexName);
    }

    public boolean hasIndices() {
        return !getIndices().isEmpty();
    }

    public IndexSchema getIndexSchemaForName(String indexName) {
        for (IndexSchema schema : getIndices()) {
            if (schema.getIndexName().equals(indexName)) {
                return schema;
            }
        }

        return null;
    }

    /**
     * Return the name of the auto increment column in the table, or null.
     *
     * @return ColumnSchema with an auto increment modifier
     */
    public String getAutoIncrementColumn() {
        for (ColumnSchema entry : getColumns()) {
            if (entry.getIsAutoIncrement()) {
                return entry.getColumnName();
            }
        }
        return null;
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

}
