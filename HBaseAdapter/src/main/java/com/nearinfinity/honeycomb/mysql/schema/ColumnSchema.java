package com.nearinfinity.honeycomb.mysql.schema;

import com.nearinfinity.honeycomb.mysql.Util;
import com.nearinfinity.honeycomb.mysql.gen.AvroColumnSchema;
import com.nearinfinity.honeycomb.mysql.gen.ColumnType;
import com.nearinfinity.honeycomb.util.Verify;
import net.jcip.annotations.Immutable;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class ColumnSchema {
    private static final DatumWriter<AvroColumnSchema> writer =
            new SpecificDatumWriter<AvroColumnSchema>(AvroColumnSchema.class);
    private static final DatumReader<AvroColumnSchema> reader =
            new SpecificDatumReader<AvroColumnSchema>(AvroColumnSchema.class);
    private final AvroColumnSchema avroColumnSchema;
    private final String columnName;

    public ColumnSchema(ColumnType type, boolean isNullable, boolean isAutoIncrement, int maxLength, int scale, int precision, String columnName) {
        checkNotNull(type);
        Verify.isNotNullOrEmpty(columnName);
        checkArgument(maxLength >= 0, "Column's max length can't be below zero.");
        checkArgument(scale >= 0, "Column's scale can't be below zero.");
        checkArgument(precision >= 0, "Column's precision can't be below zero.");
        this.avroColumnSchema = new AvroColumnSchema(type, isNullable, isAutoIncrement, maxLength, scale, precision);
        this.columnName = columnName;
    }

    /**
     * Construct a column schema based on a avro column schema and column name.
     *
     * @param columnName       Column name [Not null, Not empty]
     * @param avroColumnSchema Avro column schema [Not null]
     */
    public ColumnSchema(String columnName, AvroColumnSchema avroColumnSchema) {
        checkNotNull(avroColumnSchema);
        Verify.isNotNullOrEmpty(columnName);
        this.avroColumnSchema = AvroColumnSchema.newBuilder(avroColumnSchema).build();
        this.columnName = columnName;
    }

    /**
     * Deserialize a byte array into a column schema.
     *
     * @param serializedColumnSchema Byte array of the column schema
     * @param columnName             Column name
     * @return An column schema for the byte array.
     */
    public static ColumnSchema deserialize(byte[] serializedColumnSchema, String columnName) {
        checkNotNull(serializedColumnSchema);
        Verify.isNotNullOrEmpty(columnName);
        return new ColumnSchema(columnName, Util.deserializeAvroObject(serializedColumnSchema, reader));
    }

    public ColumnType getType() {
        return avroColumnSchema.getType();
    }

    public boolean getIsNullable() {
        return avroColumnSchema.getIsNullable();
    }

    public boolean getIsAutoIncrement() {
        return avroColumnSchema.getIsAutoIncrement();
    }

    public int getMaxLength() {
        return avroColumnSchema.getMaxLength();
    }

    /**
     * Serialize the column schema out to a byte array
     *
     * @return Serialized form of the column schema
     */
    public byte[] serialize() {
        return Util.serializeAvroObject(avroColumnSchema, writer);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ColumnSchema that = (ColumnSchema) o;

        if (avroColumnSchema != null ? !avroColumnSchema.equals(that.avroColumnSchema) : that.avroColumnSchema != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        return avroColumnSchema != null ? avroColumnSchema.hashCode() : 0;
    }

    public String getColumnName() {
        return columnName;
    }

    /**
     * Retrieve a copy of the underlying avro object used to create this index schema.
     *
     * @return Avro object representing this object.
     */
    AvroColumnSchema getAvroValue() {
        return AvroColumnSchema.newBuilder(avroColumnSchema).build();
    }
}
