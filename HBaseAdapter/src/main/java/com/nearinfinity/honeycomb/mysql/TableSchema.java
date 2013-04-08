package com.nearinfinity.honeycomb.mysql;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.nearinfinity.honeycomb.mysql.gen.AvroColumnSchema;
import com.nearinfinity.honeycomb.mysql.gen.AvroIndexSchema;
import com.nearinfinity.honeycomb.mysql.gen.AvroTableSchema;
import com.nearinfinity.honeycomb.util.Verify;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class TableSchema {
    private static final DatumWriter<AvroTableSchema> writer =
            new SpecificDatumWriter<AvroTableSchema>(AvroTableSchema.class);
    private static final DatumReader<AvroTableSchema> reader =
            new SpecificDatumReader<AvroTableSchema>(AvroTableSchema.class);
    private final AvroTableSchema avroTableSchema;
    private final Collection<ColumnSchema> columns;
    private final Collection<IndexSchema> indices;

    /**
     * Construct a table schema based on a Avro Table Schema.
     *
     * @param avroTableSchema Avro table schema [Not null]
     */
    public TableSchema(AvroTableSchema avroTableSchema) {
        checkNotNull(avroTableSchema);
        checkNotNull(avroTableSchema.getColumns());
        checkNotNull(avroTableSchema.getIndices());

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

    /**
     * Deserialize a byte array into a table schema.
     *
     * @param serializedTableSchema Byte array of the table schema [Not null]
     * @return An table schema for the byte array.
     */
    public static TableSchema deserialize(byte[] serializedTableSchema) {
        checkNotNull(serializedTableSchema);
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

    /**
     * Serialize the table schema out to a byte array
     *
     * @return Serialized form of the table schema
     */
    public byte[] serialize() {
        return Util.serializeAvroObject(avroTableSchema, writer);
    }

    /**
     * Retrieve the column schemas in the table schema
     *
     * @return Column schemas
     */
    public Collection<ColumnSchema> getColumns() {
        return Collections.unmodifiableCollection(columns);
    }

    /**
     * Retrieve the index schemas in the table schema
     *
     * @return Index schemas
     */
    public Collection<IndexSchema> getIndices() {
        return Collections.unmodifiableCollection(indices);
    }

    /**
     * Retrieve a map of column name to column schema.
     *
     * @return Column name to column schema map
     */
    public Map<String, ColumnSchema> getColumnsMap() {
        Map<String, ColumnSchema> map = Maps.newHashMap();
        for (ColumnSchema columnSchema : columns) {
            map.put(columnSchema.getColumnName(), columnSchema);
        }

        return map;
    }

    /**
     * Add an index schema to the table schema
     *
     * @param indices New index schemas [Not null]
     */
    public void addIndices(Collection<IndexSchema> indices) {
        checkNotNull(indices);
        for (IndexSchema entry : indices) {
            this.indices.add(entry);
            avroTableSchema.getIndices().put(entry.getIndexName(), entry.getAvroValue());
        }
    }

    /**
     * Remove an index schema from the table schema
     *
     * @param indexName Name of index schema [Not null, Not empty]
     */
    public void removeIndex(String indexName) {
        Verify.isNotNullOrEmpty(indexName);
        IndexSchema schema = getIndexSchemaForName(indexName);
        checkNotNull(schema);
        indices.remove(schema);
        avroTableSchema.getIndices().remove(indexName);
    }

    public boolean hasIndices() {
        return !indices.isEmpty();
    }

    /**
     * Retrieve an index schema by name.
     *
     * @param indexName Name of index schema [Not null, Not empty]
     * @return Index schema by name indexName
     */
    public IndexSchema getIndexSchemaForName(String indexName) {
        for (IndexSchema schema : indices) {
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
        for (ColumnSchema entry : columns) {
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
