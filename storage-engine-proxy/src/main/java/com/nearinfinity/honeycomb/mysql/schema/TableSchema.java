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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.nearinfinity.honeycomb.mysql.Util;
import com.nearinfinity.honeycomb.mysql.gen.AvroColumnSchema;
import com.nearinfinity.honeycomb.mysql.gen.AvroIndexSchema;
import com.nearinfinity.honeycomb.mysql.gen.AvroTableSchema;
import com.nearinfinity.honeycomb.mysql.schema.versioning.SchemaVersionUtils;
import com.nearinfinity.honeycomb.mysql.schema.versioning.TableSchemaInfo;
import com.nearinfinity.honeycomb.util.Verify;

/**
 * Stores the column and index metadata information defined on a table.
 * Internal application type used to wrap the serialized {@link AvroTableSchema} type
 */
public final class TableSchema {
    private static final DatumWriter<AvroTableSchema> writer =
            new SpecificDatumWriter<AvroTableSchema>(AvroTableSchema.class);
    private static final DatumReader<AvroTableSchema> reader =
            new SpecificDatumReader<AvroTableSchema>(AvroTableSchema.class);
    private final AvroTableSchema avroTableSchema;
    private final Collection<ColumnSchema> columns;
    private final Collection<IndexSchema> indices;
    private int uniqueIndexCount = 0;

    /**
     * Construct a table schema from a map of column schemas and index schemas.
     *
     * @param columns Column schemas map [Not null]
     * @param indices Index schema map [Not null]
     */
    public TableSchema(Collection<ColumnSchema> columns, Collection<IndexSchema> indices) {
        checkArgument(columns.size() > 0, "Table must have at least one column.");
        checkNotNull(indices);

        Map<String, AvroColumnSchema> columnSchemaMap = Maps.newHashMap();
        for (ColumnSchema columnSchema : columns) {
            columnSchemaMap.put(columnSchema.getColumnName(), columnSchema.getAvroValue());
        }

        Map<String, AvroIndexSchema> indexSchemaMap = Maps.newHashMap();
        for (IndexSchema indexSchema : indices) {
            indexSchemaMap.put(indexSchema.getIndexName(), indexSchema.getAvroValue());
            if (indexSchema.getIsUnique()) {
                uniqueIndexCount++;
            }
        }

        avroTableSchema = AvroTableSchema.newBuilder()
                .setColumns(columnSchemaMap)
                .setIndices(indexSchemaMap)
                .build();

        this.columns = columns;
        this.indices = indices;
    }

    private TableSchema(AvroTableSchema avroTableSchema) {
        this.avroTableSchema = avroTableSchema;

        columns = Lists.newArrayList();
        for (Map.Entry<String, AvroColumnSchema> entry : avroTableSchema.getColumns().entrySet()) {
            columns.add(new ColumnSchema(entry.getKey(), entry.getValue()));
        }

        indices = Lists.newArrayList();
        for (Map.Entry<String, AvroIndexSchema> entry : avroTableSchema.getIndices().entrySet()) {
            indices.add(new IndexSchema(entry.getKey(), entry.getValue()));
            if (entry.getValue().getIsUnique()) {
                uniqueIndexCount++;
            }
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
        checkArgument(serializedTableSchema.length > 0);

        SchemaVersionUtils.processSchemaVersion(serializedTableSchema[0], TableSchemaInfo.VER_CURRENT);

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
     * Add an index schema to the table schema
     *
     * @param indices New index schemas [Not null]
     */
    public void addIndices(Collection<IndexSchema> indices) {
        checkNotNull(indices);
        for (IndexSchema entry : indices) {
            this.indices.add(entry);
            avroTableSchema.getIndices().put(entry.getIndexName(), entry.getAvroValue());
            if (entry.getIsUnique()) {
                uniqueIndexCount++;
            }
        }
    }

    /**
     * Remove an index schema from the table schema
     *
     * @param indexName Name of index schema [Not null, Not empty]
     */
    public void removeIndex(String indexName) {
        Verify.isNotNullOrEmpty(indexName);
        IndexSchema schema = getIndexSchema(indexName);
        checkNotNull(schema);
        indices.remove(schema);
        avroTableSchema.getIndices().remove(indexName);
        if (schema.getIsUnique()) {
            uniqueIndexCount--;
        }
    }

    public boolean hasIndices() {
        return !indices.isEmpty();
    }

    public boolean hasUniqueIndices() {
        return uniqueIndexCount > 0;
    }

    /**
     * Retrieve a column schema by name.
     *
     * @param columnName Name of index schema [Not null, Not empty]
     * @return Index schema by name indexName
     */
    public ColumnSchema getColumnSchema(String columnName) {
        Verify.isNotNullOrEmpty(columnName);
        AvroColumnSchema columnSchema = avroTableSchema.getColumns().get(columnName);
        return new ColumnSchema(columnName, columnSchema);
    }

    /**
     * Retrieve an index schema by name.
     *
     * @param indexName Name of index schema [Not null, Not empty]
     * @return Index schema by name indexName
     */
    public IndexSchema getIndexSchema(String indexName) {
        Verify.isNotNullOrEmpty(indexName);
        AvroIndexSchema indexSchema = avroTableSchema.getIndices().get(indexName);
        return new IndexSchema(indexName, indexSchema);
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
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TableSchema that = (TableSchema) o;

        return avroTableSchema == null ? that.avroTableSchema == null
                : avroTableSchema.equals(that.avroTableSchema);
    }

    @Override
    public int hashCode() {
        return avroTableSchema != null ? avroTableSchema.hashCode() : 0;
    }

    @Override
    public String toString() {
        final ToStringHelper helper = Objects.toStringHelper(this.getClass());

        if( avroTableSchema != null ) {
            helper.add("Version", avroTableSchema.getVersion());
            helper.add("Columns", avroTableSchema.getColumns());
            helper.add("Indices", avroTableSchema.getIndices());
        }

        return helper.toString();
    }
}
