package com.nearinfinity.honeycomb.mysql;

import com.nearinfinity.honeycomb.mysql.gen.AvroColumnSchema;
import com.nearinfinity.honeycomb.mysql.gen.AvroColumnSchema;
import com.nearinfinity.honeycomb.mysql.gen.ColumnType;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

public class ColumnSchema {
    private static final DatumWriter<AvroColumnSchema> writer =
            new SpecificDatumWriter<AvroColumnSchema>(AvroColumnSchema.class);
    private static final DatumReader<AvroColumnSchema> reader =
            new SpecificDatumReader<AvroColumnSchema>(AvroColumnSchema.class);
    private final AvroColumnSchema avroColumnSchema;

    public ColumnSchema() {
        avroColumnSchema = new AvroColumnSchema(ColumnType.LONG, false, false, 0, 0, 0);
    }

    public ColumnSchema(AvroColumnSchema avroColumnSchema) {
        this.avroColumnSchema = avroColumnSchema;
    }

    public ColumnSchema(ColumnType type, boolean isNullable, boolean isAutoIncrement, int maxLength, int scale, int precision) {
        avroColumnSchema = new AvroColumnSchema(type, isNullable, isAutoIncrement, maxLength, scale, precision);
    }

    public static ColumnSchema deserialize(byte[] serializedColumnSchema) {
        return new ColumnSchema(Util.deserializeAvroObject(serializedColumnSchema, reader));
    }

    public ColumnType getType() {
        return avroColumnSchema.getType();
    }

    public void setType(ColumnType value) {
        avroColumnSchema.setType(value);
    }

    public boolean getIsNullable() {
        return avroColumnSchema.getIsNullable();
    }

    public void setIsNullable(boolean value) {
        avroColumnSchema.setIsNullable(value);
    }

    public boolean getIsAutoIncrement() {
        return avroColumnSchema.getIsAutoIncrement();
    }

    public void setIsAutoIncrement(boolean value) {
        avroColumnSchema.setIsAutoIncrement(value);
    }

    public Integer getMaxLength() {
        return avroColumnSchema.getMaxLength();
    }

    public void setMaxLength(Integer value) {
        avroColumnSchema.setMaxLength(value);
    }

    public Integer getScale() {
        return avroColumnSchema.getScale();
    }

    public void setScale(Integer value) {
        avroColumnSchema.setScale(value);
    }

    public Integer getPrecision() {
        return avroColumnSchema.getPrecision();
    }

    public void setPrecision(Integer value) {
        avroColumnSchema.setPrecision(value);
    }

    public AvroColumnSchema getAvroValue() {
        return avroColumnSchema;
    }

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
}
