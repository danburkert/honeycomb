package com.nearinfinity.honeycomb.mysql;

import com.nearinfinity.honeycomb.mysql.gen.AvroIndexSchema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.util.List;


public class IndexSchema {
    private static final DatumWriter<AvroIndexSchema> writer =
            new SpecificDatumWriter<AvroIndexSchema>(AvroIndexSchema.class);
    private static final DatumReader<AvroIndexSchema> reader =
            new SpecificDatumReader<AvroIndexSchema>(AvroIndexSchema.class);
    private final AvroIndexSchema avroIndexSchema;

    public IndexSchema() {
        avroIndexSchema = new AvroIndexSchema();
    }

    public IndexSchema(List<String> columns, boolean isUnique) {
        avroIndexSchema = new AvroIndexSchema(columns, isUnique);
    }

    public IndexSchema(AvroIndexSchema avroIndexSchema) {
        this.avroIndexSchema = avroIndexSchema;
    }

    public static IndexSchema deserialize(byte[] serializedIndexSchema) {
        return new IndexSchema(Util.deserializeAvroObject(serializedIndexSchema, reader));
    }

    public List<String> getColumns() {
        return avroIndexSchema.getColumns();
    }

    public boolean getIsUnique() {
        return avroIndexSchema.getIsUnique();
    }

    public AvroIndexSchema getAvroValue() {
        return avroIndexSchema;
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
