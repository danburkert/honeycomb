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
 * Copyright 2013 Near Infinity Corporation.
 */


package com.nearinfinity.honeycomb.mysql;


import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.nearinfinity.honeycomb.mysql.gen.AvroRow;
import com.nearinfinity.honeycomb.mysql.gen.UUIDContainer;
import com.nearinfinity.honeycomb.mysql.schema.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.schema.TableSchema;
import com.nearinfinity.honeycomb.mysql.schema.versioning.RowSchemaInfo;
import com.nearinfinity.honeycomb.mysql.schema.versioning.SchemaVersionUtils;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

/**
 * Stores the row metadata information for a row defined in a table.
 * Internal application type used to wrap the serialized {@link AvroRow} type
 */
public class Row {
    private static final DatumWriter<AvroRow> writer =
            new SpecificDatumWriter<AvroRow>(AvroRow.class);
    private static final DatumReader<AvroRow> reader =
            new SpecificDatumReader<AvroRow>(AvroRow.class);
    private final AvroRow row;
    private TableSchema tableSchema;

    /**
     * Construct a new Row with specified records and UUID.
     *
     * @param records Map of column name to record value
     * @param uuid    UUID representing the unique position of the Row
     * @param schema  Table schema for the row
     */
    public Row(Map<String, ByteBuffer> records, UUID uuid, TableSchema schema) {
        checkNotNull(records, "records must not be null.");
        checkNotNull(schema, "schema must not be null.");

        Collection<ColumnSchema> columns = schema.getColumns();
        // uuid nullity will be checked by UUIDToBytes
        List<ByteBuffer> recordList = Lists.newArrayList();
        for (ColumnSchema columnSchema : columns) {
            recordList.add(records.get(columnSchema.getColumnName()));
        }

        this.tableSchema = schema;
        row = AvroRow.newBuilder()
                .setUuid(new UUIDContainer(Util.UUIDToBytes(uuid)))
                .setRecords(recordList)
                .build();
    }

    /**
     * Constructor called during deserialization.
     *
     * @param row {@link AvroRow} the underlying content for this row
     */
    private Row(AvroRow row, TableSchema tableSchema) {
        this.row = row;
        this.tableSchema = tableSchema;
    }

    /**
     * Deserialize the provided serialized row buffer to a new {@link Row} instance
     *
     * @param serializedRow byte buffer containing serialized Row
     * @param tableSchema   schema describing the serialized row
     * @return new Row instance from serializedRow
     */
    public static Row deserialize(byte[] serializedRow, TableSchema tableSchema) {
        checkNotNull(serializedRow);
        checkNotNull(tableSchema);
        checkArgument(serializedRow.length > 0);

        SchemaVersionUtils.processSchemaVersion(serializedRow[0], RowSchemaInfo.VER_CURRENT);

        return new Row(Util.deserializeAvroObject(serializedRow, reader), tableSchema);
    }

    public static byte[] updateSerializedSchema(byte[] row, TableSchema tableSchema) {
        byte version = row[0];
        if (isMostRecentVersion(version)) {
            return row;
        }

        // Bring the row up to most recent version
        return Row.deserialize(row, tableSchema).serialize();
    }

    private static boolean isMostRecentVersion(byte version) {
        return SchemaVersionUtils.decodeAvroSchemaVersion(version) == RowSchemaInfo.VER_CURRENT;
    }

    /**
     * Returns the {@link UUID} of this Row.
     *
     * @return UUID of this Row.
     */
    public UUID getUUID() {
        return Util.bytesToUUID(row.getUuid().bytes());
    }

    public void setUUID(UUID uuid) {
        row.setUuid(new UUIDContainer(Util.UUIDToBytes(uuid)));
    }

    /**
     * Set UUID to a new random UUID
     */
    public void setRandomUUID() {
        row.setUuid(new UUIDContainer(Util.UUIDToBytes(UUID.randomUUID())));
    }

    /**
     * Returns the a map of column names to records of this Row.
     *
     * @return Map of column names to records
     */
    public Map<String, ByteBuffer> getRecords() {
        Map<String, ByteBuffer> records = Maps.newHashMap();
        List<ByteBuffer> orderedRecords = row.getRecords();
        int i = 0;
        for (ColumnSchema columnSchema : tableSchema.getColumns()) {
            ByteBuffer value = orderedRecords.get(i++);
            if (value != null) {
                records.put(columnSchema.getColumnName(), value);
            }
        }

        return records;
    }

    /**
     * Serialize this {@link Row} instance to a byte array.
     *
     * @return Serialized row
     */
    public byte[] serialize() {
        return Util.serializeAvroObject(row, writer);
    }

    @Override
    public int hashCode() {
        return row.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null) {
            return false;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        Row other = (Row) obj;
        if (row == null) {
            if (other.row != null) {
                return false;
            }
        } else if (!row.equals(other.row)) {
            return false;
        }

        return true;
    }

    @Override
    public String toString() {
        final ToStringHelper toString = Objects.toStringHelper(this.getClass());

        toString.add("Version", row.getVersion())
                .add("UUID", getUUID());

        for (final Map.Entry<String, ByteBuffer> entry : getRecords().entrySet()) {
            toString.add("Record", format("%s: %s", entry.getKey(), entry.getValue()));
        }

        return toString.toString();
    }

    /**
     * Update a column's value in the row.
     * @param column Column name
     * @param value New value for the column
     */
    public void updateColumn(String column, ByteBuffer value) {
        checkNotNull(column);
        int i = 0;
        for (ColumnSchema schema : tableSchema.getColumns()){
            if (schema.getColumnName().equals(column)){
                row.getRecords().set(i, value);
                break;
            }
            i++;
        }
    }
}
