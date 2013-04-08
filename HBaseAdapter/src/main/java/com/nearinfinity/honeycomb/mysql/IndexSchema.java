package com.nearinfinity.honeycomb.mysql;

import com.google.common.collect.ImmutableList;
import com.nearinfinity.honeycomb.mysql.gen.AvroIndexSchema;
import com.nearinfinity.honeycomb.util.Verify;
import net.jcip.annotations.Immutable;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class IndexSchema {
    private static final DatumWriter<AvroIndexSchema> writer =
            new SpecificDatumWriter<AvroIndexSchema>(AvroIndexSchema.class);
    private static final DatumReader<AvroIndexSchema> reader =
            new SpecificDatumReader<AvroIndexSchema>(AvroIndexSchema.class);
    private final AvroIndexSchema avroIndexSchema;
    private final String indexName;

    public IndexSchema(List<String> columns, boolean isUnique, String indexName) {
        checkNotNull(columns);
        Verify.isNotNullOrEmpty(indexName);
        this.avroIndexSchema = new AvroIndexSchema(columns, isUnique);
        this.indexName = indexName;
    }

    /**
     * Construct an index schema based on a avro index schema and index name
     *
     * @param avroIndexSchema Avro index schema [Not null]
     * @param indexName       Index name [Not null, Not empty]
     */
    IndexSchema(AvroIndexSchema avroIndexSchema, String indexName) {
        checkNotNull(avroIndexSchema);
        Verify.isNotNullOrEmpty(indexName);
        this.avroIndexSchema = AvroIndexSchema.newBuilder(avroIndexSchema).build();
        this.indexName = indexName;
    }

    /**
     * Deserialize a byte array into an index schema.
     *
     * @param serializedIndexSchema Byte array of the index schema [Not null]
     * @param indexName             Name of the index [Not null, Not empty]
     * @return An index schema for the byte array.
     */
    public static IndexSchema deserialize(byte[] serializedIndexSchema, String indexName) {
        checkNotNull(serializedIndexSchema);
        Verify.isNotNullOrEmpty(indexName);
        return new IndexSchema(Util.deserializeAvroObject(serializedIndexSchema, reader), indexName);
    }

    /**
     * Retrieve the name of this index.
     *
     * @return Index name
     */
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

    /**
     * Retrieve whether this index is unique.
     *
     * @return Is a unique index?
     */
    public boolean getIsUnique() {
        return avroIndexSchema.getIsUnique();
    }

    /**
     * Retrieve a copy of the underlying avro object used to create this index schema.
     *
     * @return Avro object representing this object.
     */
    AvroIndexSchema getAvroValue() {
        return AvroIndexSchema.newBuilder(avroIndexSchema).build();
    }

    /**
     * Serialize the index schema out to a byte array
     *
     * @return Serialized form of the index schema
     */
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
