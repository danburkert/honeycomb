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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import net.jcip.annotations.Immutable;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;
import com.google.common.collect.ImmutableList;
import com.nearinfinity.honeycomb.mysql.Util;
import com.nearinfinity.honeycomb.mysql.gen.AvroIndexSchema;
import com.nearinfinity.honeycomb.util.Verify;

/**
 * Stores the index information for indexed column(s) in a table.
 * Internal application type used to wrap the serialized {@link AvroIndexSchema} type
 */
@Immutable
public final class IndexSchema {
    private static final DatumWriter<AvroIndexSchema> writer =
            new SpecificDatumWriter<AvroIndexSchema>(AvroIndexSchema.class);
    private static final DatumReader<AvroIndexSchema> reader =
            new SpecificDatumReader<AvroIndexSchema>(AvroIndexSchema.class);
    private final AvroIndexSchema avroIndexSchema;
    private final String indexName;

    /**
     * Construct an index schema for columns in a table.
     *
     * @param indexName Name of the index
     * @param columns   Table columns
     * @param isUnique  Is a unique index?
     */
    public IndexSchema(String indexName, List<String> columns, boolean isUnique) {
        checkNotNull(columns);
        Verify.isNotNullOrEmpty(indexName);
        avroIndexSchema = new AvroIndexSchema(ImmutableList.copyOf(columns), isUnique);
        this.indexName = indexName;
    }

    /**
     * Construct an index schema based on a avro index schema and index name
     *
     * @param indexName       Index name [Not null, Not empty]
     * @param avroIndexSchema Avro index schema [Not null]
     */
    IndexSchema(String indexName, AvroIndexSchema avroIndexSchema) {
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
        return new IndexSchema(indexName, Util.deserializeAvroObject(serializedIndexSchema, reader));
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
     * Serialize the index schema out to a byte array
     *
     * @return Serialized form of the index schema
     */
    public byte[] serialize() {
        return Util.serializeAvroObject(avroIndexSchema, writer);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IndexSchema that = (IndexSchema) o;

        if (avroIndexSchema == null) {
            return that.avroIndexSchema == null;
        }

        return avroIndexSchema.equals(that.avroIndexSchema);
    }

    @Override
    public int hashCode() {
        return avroIndexSchema != null ? avroIndexSchema.hashCode() : 0;
    }

    /**
     * Retrieve a copy of the underlying avro object used to create this index schema.
     *
     * @return Avro object representing this object.
     */
    AvroIndexSchema getAvroValue() {
        return AvroIndexSchema.newBuilder(avroIndexSchema).build();
    }

    @Override
    public String toString() {
        final ToStringHelper helper = Objects.toStringHelper(this.getClass())
                .add("name", indexName);

        if( avroIndexSchema != null ) {
            helper.add("columns", avroIndexSchema.getColumns());
            helper.add("isUnique", avroIndexSchema.getIsUnique());
        }

        return helper.toString();
    }
}
