package com.nearinfinity.honeycomb.mysql;

import com.google.common.collect.ImmutableList;
import com.nearinfinity.honeycomb.mysql.gen.AvroIndexSchema;
import net.jcip.annotations.Immutable;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.util.List;

@Immutable
public class IndexSchema {
    private static final DatumWriter<AvroIndexSchema> writer =
            new SpecificDatumWriter<AvroIndexSchema>(AvroIndexSchema.class);
    private static final DatumReader<AvroIndexSchema> reader =
            new SpecificDatumReader<AvroIndexSchema>(AvroIndexSchema.class);
    private final AvroIndexSchema avroIndexSchema;
    private final String indexName;

    public IndexSchema(AvroIndexSchema avroIndexSchema, String indexName) {
        this.avroIndexSchema = AvroIndexSchema.newBuilder(avroIndexSchema).build();
        this.indexName = indexName;
    }

    public static IndexSchema deserialize(byte[] serializedIndexSchema, String indexName) {
        return new IndexSchema(Util.deserializeAvroObject(serializedIndexSchema, reader), indexName);
    }

    public String getIndexName() {
        return indexName;
    }

    /**
     * Return the columns in the index.
     *
     * @return Columns in index schema [Immutable]
     */
    public List<String> getColumns() {
        return ImmutableList.copyOf(avroIndexSchema.getColumns());
    }

    public boolean getIsUnique() {
        return avroIndexSchema.getIsUnique();
    }

    /**
     * Retrieve a copy of the underlying avro object used to create this index schema.
     *
     * @return Avro object representing this object.
     */
    public AvroIndexSchema getAvroValue() {
        return AvroIndexSchema.newBuilder(avroIndexSchema).build();
    }

    public byte[] serialize() {
        return Util.serializeAvroObject(avroIndexSchema, writer);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexSchema that = (IndexSchema) o;

        if (avroIndexSchema != null ? !avroIndexSchema.equals(that.avroIndexSchema) : that.avroIndexSchema != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        return avroIndexSchema != null ? avroIndexSchema.hashCode() : 0;
    }
}
