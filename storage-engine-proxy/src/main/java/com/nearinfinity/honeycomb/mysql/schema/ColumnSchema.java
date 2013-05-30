/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * Copyright 2013 Altamira Corporation.
 */


package com.nearinfinity.honeycomb.mysql.schema;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import net.jcip.annotations.Immutable;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;
import com.nearinfinity.honeycomb.mysql.Util;
import com.nearinfinity.honeycomb.mysql.gen.AvroColumnSchema;
import com.nearinfinity.honeycomb.mysql.gen.ColumnType;
import com.nearinfinity.honeycomb.util.Verify;

/**
 * Stores the column metadata details for a column in a table.
 * Internal application type used to wrap the serialized {@link AvroColumnSchema} type
 */
@Immutable
public final class ColumnSchema {
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
    private ColumnSchema(String columnName,
                         ColumnType type,
                         boolean isNullable,
                         boolean isAutoIncrement,
                         Integer maxLength,
                         Integer scale,
                         Integer precision) {
        Verify.isNotNullOrEmpty(columnName);
        checkNotNull(type);

        if(type == ColumnType.DECIMAL) {
            checkArgument(scale >= 0, "Scale may not be null or negative.");
            checkArgument(precision >= 0, "Precision may not be null or negative.");
        } else {
            checkArgument(scale == null, "Scale must be null for non-decimal column.");
            checkArgument(precision == null, "Precision must be null for non-decimal column.");
        }

        if(type == ColumnType.BINARY || type == ColumnType.STRING) {
            checkArgument(maxLength >= 0, "maxLength may not be null or negative.");
        } else {
            checkArgument(maxLength == null, "maxLength must be null for non-variable length column");
        }

        if(isAutoIncrement) {
            checkArgument(type == ColumnType.LONG
                       || type == ColumnType.DOUBLE
                       || type == ColumnType.ULONG,
                    "Only integer or floating-point columns may be auto-increment.");
        }

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
    ColumnSchema(String columnName, AvroColumnSchema avroColumnSchema) {
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
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ColumnSchema that = (ColumnSchema) o;

        if (avroColumnSchema != null ? !avroColumnSchema.equals(that.avroColumnSchema) : that.avroColumnSchema != null) {
            return false;
        }

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
        final ToStringHelper helper = Objects.toStringHelper(this.getClass())
                .add("name", columnName);

        if( avroColumnSchema != null ) {
            helper.add("type", avroColumnSchema.getType());
            helper.add("isNullable", avroColumnSchema.getIsNullable());
            helper.add("isAutoIncrement", avroColumnSchema.getIsAutoIncrement());
            helper.add("maxLength", avroColumnSchema.getMaxLength());
            helper.add("precision", avroColumnSchema.getPrecision());
            helper.add("scale", avroColumnSchema.getScale());
        }

        return helper.toString();
    }

    /**
     * Create a {@link ColumnSchema} builder with given column name and type.
     * @param columnName
     * @param type
     * @return A builder object used for schema creation
     */
    public static Builder builder(String columnName, ColumnType type) {
        return new Builder(columnName, type);
    }

    /**
     * Builder for ColumnSchema.  By default, the column schema is nullable and
     * non auto increment.
     */
    public static class Builder {
        final String columnName;
        final ColumnType type;
        boolean isNullable = true;
        boolean isAutoIncrement = false;
        Integer maxLength = null;
        Integer scale = null;
        Integer precision = null;

        /**
         * Default constructor, equivalent to ColumnSchema.builder().
         * @param columnName
         * @param type
         */
        public Builder(String columnName, ColumnType type) {
            this.columnName = columnName;
            this.type = type;
        }

        public Builder setIsNullable(boolean isNullable) {
            this.isNullable = isNullable;
            return this;
        }

        public Builder setIsAutoIncrement(boolean isAutoIncrement) {
            this.isAutoIncrement = isAutoIncrement;
            return this;
        }

        public Builder setMaxLength(int maxLength) {
            this.maxLength = maxLength;
            return this;
        }

        public Builder setScale(int scale) {
            this.scale = scale;
            return this;
        }

        public Builder setPrecision(int precision) {
            this.precision = precision;
            return this;
        }

        public ColumnSchema build() {
            return new ColumnSchema(columnName, type, isNullable,
                    isAutoIncrement, maxLength, scale, precision);
        }
    }
}