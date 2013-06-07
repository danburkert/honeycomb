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


package com.nearinfinity.honeycomb.mysql.schema;

import static org.junit.Assert.assertEquals;
import nl.jqno.equalsverifier.EqualsVerifier;

import org.junit.Test;

import com.nearinfinity.honeycomb.mysql.gen.ColumnType;

public class ColumnSchemaTest {

    private static final String TEST_COLUMN = "testColumn";
    private static final ColumnSchema TEST_COL_SCHEMA = ColumnSchema.builder(TEST_COLUMN, ColumnType.ULONG).build();

    @Test(expected = NullPointerException.class)
    public void testColumnSchemaCreationInvalidColumnSchema() {
        new ColumnSchema(TEST_COLUMN, null);
    }

    @Test(expected = NullPointerException.class)
    public void testDeserializeNullSerializedSchema() {
        ColumnSchema.deserialize(null, TEST_COLUMN);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDeserializeEmptyColumnName() {
        ColumnSchema.deserialize(TEST_COL_SCHEMA.serialize(), "");
    }

    @Test(expected = NullPointerException.class)
    public void testDeserializeInvalidColumnName() {
        ColumnSchema.deserialize(TEST_COL_SCHEMA.serialize(), null);
    }

    @Test
    public void testDeserializeValidSerializedSchemaAndColumnName() {
        final ColumnSchema actualSchema = ColumnSchema.deserialize(TEST_COL_SCHEMA.serialize(),
                TEST_COL_SCHEMA.getColumnName());

        assertEquals(TEST_COL_SCHEMA.getColumnName(), actualSchema.getColumnName());
        assertEquals(TEST_COL_SCHEMA, actualSchema);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildDecimalColumnInvalidScale() {
        new ColumnSchema.Builder(TEST_COLUMN, ColumnType.DECIMAL).setScale(-1).setPrecision(0).build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildDecimalColumnInvalidPrecision() {
        new ColumnSchema.Builder(TEST_COLUMN, ColumnType.DECIMAL).setScale(0).setPrecision(-1).build();
    }

    @Test
    public void testBuildValidDecimalColumn() {
        new ColumnSchema.Builder(TEST_COLUMN, ColumnType.DECIMAL).setScale(0).setPrecision(0).build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildBinaryColumnWithScale() {
        new ColumnSchema.Builder(TEST_COLUMN, ColumnType.BINARY).setScale(0).build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildBinaryColumnWithPrecision() {
        new ColumnSchema.Builder(TEST_COLUMN, ColumnType.BINARY).setPrecision(0).build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildBinaryColumnInvalidMaxLength() {
        new ColumnSchema.Builder(TEST_COLUMN, ColumnType.BINARY).setMaxLength(-1).build();
    }

    @Test
    public void testBuildBinaryColumnValidMaxLength() {
        new ColumnSchema.Builder(TEST_COLUMN, ColumnType.BINARY).setMaxLength(0).build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildStringColumnInvalidMaxLength() {
        new ColumnSchema.Builder(TEST_COLUMN, ColumnType.STRING).setMaxLength(-1).build();
    }

    @Test
    public void testBuildStringColumnValidMaxLength() {
        new ColumnSchema.Builder(TEST_COLUMN, ColumnType.STRING).setMaxLength(0).build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildDecimalColumnWithMaxLength() {
        new ColumnSchema.Builder(TEST_COLUMN, ColumnType.DECIMAL).setScale(0).setPrecision(0).setMaxLength(0).build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildDecimalColumnAsAutoIncrement() {
        new ColumnSchema.Builder(TEST_COLUMN, ColumnType.DECIMAL).setScale(0).setPrecision(0).setIsNullable(false)
                .setIsAutoIncrement(true).build();
    }

    @Test
    public void testBuildLongColumnAsAutoIncrement() {
        new ColumnSchema.Builder(TEST_COLUMN, ColumnType.LONG).setIsAutoIncrement(true).build();
    }

    @Test
    public void testBuildDoubleColumnAsAutoIncrement() {
        new ColumnSchema.Builder(TEST_COLUMN, ColumnType.DOUBLE).setIsAutoIncrement(true).build();
    }

    @Test
    public void testBuildULongColumnAsAutoIncrement() {
        new ColumnSchema.Builder(TEST_COLUMN, ColumnType.ULONG).setIsAutoIncrement(true).build();
    }

    @Test
    public void equalsContract() {
        EqualsVerifier.forClass(ColumnSchema.class).verify();
    }
}
