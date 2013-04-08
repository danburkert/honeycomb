package com.nearinfinity.honeycomb.mysql.schema;

import com.google.common.base.Objects;
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

    /**
     * Construct a column with column data.
     *
     * @param columnName      Column name
     * @param type            Column type
     * @param isNullable      Is nullable column
     * @param isAutoIncrement Is auto increment column
     * @param maxLength       Column max length
     * @param scale           Column scale
     * @param precision       Column precision
     */
    public ColumnSchema(String columnName, ColumnType type, boolean isNullable,
                        boolean isAutoIncrement, Integer maxLength,
                        Integer scale, Integer precision) {
        checkNotNull(type);
        Verify.isNotNullOrEmpty(columnName);
        checkArgument(maxLength == null || maxLength >= 0, "Max length can't be below zero.");
        checkArgument(scale == null || scale >= 0, "Scale can't be below zero.");
        checkArgument(precision == null || precision >= 0, "Precision can't be below zero.");
        avroColumnSchema = new AvroColumnSchema(ColumnType.valueOf(type.name()),
                isNullable, isAutoIncrement, maxLength, scale, precision);
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

    public int getPrecision() {
        return avroColumnSchema.getPrecision();
    }

    public int getScale() {
        return avroColumnSchema.getScale();
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

    @Override
    public String toString() {
        return Objects.toStringHelper(this.getClass())
                .add("name", columnName)
                .add("isNullable", avroColumnSchema.getIsNullable())
                .add("isAutoIncrement", avroColumnSchema.getIsAutoIncrement())
                .add("maxLength", avroColumnSchema.getMaxLength())
                .add("precision", avroColumnSchema.getPrecision())
                .add("scale", avroColumnSchema.getScale())
                .toString();
    }
}
