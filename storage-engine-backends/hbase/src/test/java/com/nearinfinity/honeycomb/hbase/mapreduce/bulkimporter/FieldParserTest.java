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


package com.nearinfinity.honeycomb.hbase.mapreduce.bulkimporter;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Longs;
import com.nearinfinity.honeycomb.mysql.gen.ColumnType;
import com.nearinfinity.honeycomb.mysql.schema.ColumnSchema;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.text.ParseException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Provides test cases for the {@link com.nearinfinity.honeycomb.hbase.mapreduce.bulkimporter.FieldParser} class. All test values used
 * for column type data comes from the valid datatype ranges of the database in
 * which the values were received from
 */
public class FieldParserTest {

    private static final String COLUMN_NAME = "c1";
    private static final String EMPTY_STRING = "";

    @Test(expected = NullPointerException.class)
    public void testParseNullValue() throws ParseException {
        FieldParser.parse(null, ColumnSchema.builder(COLUMN_NAME, ColumnType.LONG).build());
    }

    @Test(expected = NullPointerException.class)
    public void testParseNullMetadata() throws ParseException {
        FieldParser.parse("foo", null);
    }

    /**
     * Tests a parse request with an empty value and a nullable
     * {@link ColumnSchema} with a {@link ColumnType} not equal to
     * {@link ColumnType#STRING} or {@link ColumnType#BINARY}
     *
     * @throws ParseException
     */
    @Test
    public void testParseEmptyValueNullableMetadataLongType()
            throws ParseException {
        ColumnSchema schema = ColumnSchema
                .builder(COLUMN_NAME, ColumnType.LONG)
                .build();

        assertNull(FieldParser.parse(EMPTY_STRING, schema));
    }

    /**
     * Tests a parse request with an empty value and a non-nullable
     * {@link ColumnSchema} with a {@link ColumnType} not equal to
     * {@link ColumnType#STRING} or {@link ColumnType#BINARY}
     *
     * @throws ParseException
     */
    @Test(expected = IllegalArgumentException.class)
    public void testParseEmptyValueNonNullableSchemaLongType()
            throws ParseException {
        ColumnSchema schema = ColumnSchema
                .builder(COLUMN_NAME, ColumnType.LONG)
                .setIsNullable(false)
                .build();

        assertNull(FieldParser.parse(EMPTY_STRING, schema));
    }

    @Test
    public void testParseLong() throws ParseException {
        ColumnSchema schema = ColumnSchema
                .builder(COLUMN_NAME, ColumnType.LONG)
                .build();

        assertEquals(ByteBuffer.wrap(Longs.toByteArray(0x00L)),
                FieldParser.parse("0", schema));

        assertEquals(ByteBuffer.wrap(Longs.toByteArray(0xFFFFFFFFFFFFFF85L)),
                FieldParser.parse("-123", schema));

        assertEquals(ByteBuffer.wrap(Longs.toByteArray(0x7BL)),
                FieldParser.parse("123", schema));

        assertEquals(ByteBuffer.wrap(Longs.toByteArray(0x7FFFFFFFFFFFFFFFL)),
                FieldParser.parse("9223372036854775807", schema));

        assertEquals(ByteBuffer.wrap(Longs.toByteArray(0x8000000000000000L)),
                FieldParser.parse("-9223372036854775808", schema));
    }

    @Test
    public void testParseULong() throws ParseException {
        ColumnSchema schema = ColumnSchema
                .builder(COLUMN_NAME, ColumnType.ULONG)
                .build();

        assertEquals(ByteBuffer.wrap(Longs.toByteArray(0x00L)),
                FieldParser.parse("0", schema));

        assertEquals(ByteBuffer.wrap(Longs.toByteArray(0x7BL)),
                FieldParser.parse("123", schema));

        assertEquals(ByteBuffer.wrap(Longs.toByteArray(0xFFFFFFFFFFFFFFFFL)),
                FieldParser.parse("18446744073709551615", schema));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseULongNegativeInput() throws ParseException {
        ColumnSchema schema = ColumnSchema
                .builder(COLUMN_NAME, ColumnType.ULONG)
                .build();

        FieldParser.parse("-123", schema);
    }

    @Test
    public void testParseDoubleZeroValue() throws ParseException {
        ColumnSchema schema = ColumnSchema
                .builder(COLUMN_NAME, ColumnType.DOUBLE)
                .build();

        // Note: These values are all big endian, as per the JVM
        assertEquals(ByteBuffer.wrap(Bytes.toBytes(0x00L)),
                FieldParser.parse("0.0", schema));

        assertEquals(ByteBuffer.wrap(Longs.toByteArray(0x40283D70A3D70A3DL)),
                FieldParser.parse("12.12", schema));

        assertEquals(ByteBuffer.wrap(Longs.toByteArray(0xC0283D70A3D70A3DL)),
                FieldParser.parse("-12.12", schema));
    }

    @Test
    public void testParseValidDateFormats() throws ParseException {
        ColumnSchema schema = ColumnSchema
                .builder(COLUMN_NAME, ColumnType.DATE)
                .build();

        final String expectedParsedDate = "1989-05-13";

        final List<String> formats = ImmutableList.of(expectedParsedDate,
                "1989.05.13", "1989/05/13", "19890513");

        for (final String format : formats) {
            assertEquals(ByteBuffer.wrap(expectedParsedDate.getBytes()),
                    FieldParser.parse(format, schema));
        }
    }

    @Test(expected = ParseException.class)
    public void testParseInvalidDateFormat() throws ParseException {
        ColumnSchema schema = ColumnSchema
                .builder(COLUMN_NAME, ColumnType.DATE)
                .build();

        FieldParser.parse("1989_05_13", schema);
    }

    @Test
    public void testParseTime() throws Exception {
        ColumnSchema schema = ColumnSchema
                .builder(COLUMN_NAME, ColumnType.TIME)
                .build();

        final String expectedParsedTime = "07:32:15";

        final List<String> formats = ImmutableList.of(expectedParsedTime,
                "073215");

        for (final String format : formats) {
            assertEquals(ByteBuffer.wrap(expectedParsedTime.getBytes()),
                    FieldParser.parse(format, schema));
        }
    }

    @Test(expected = ParseException.class)
    public void testParseInvalidTimeFormat() throws ParseException {
        ColumnSchema schema = ColumnSchema
                .builder(COLUMN_NAME, ColumnType.TIME)
                .build();

        FieldParser.parse("07_32_15", schema);
    }

    @Test
    public void testParseDateTime() throws Exception {
        ColumnSchema schema = ColumnSchema
                .builder(COLUMN_NAME, ColumnType.DATETIME)
                .build();

        final String expectedParsedDateTime = "1989-05-13 07:32:15";

        final List<String> formats = ImmutableList
                .of(expectedParsedDateTime, "1989.05.13 07:32:15",
                        "1989/05/13 07:32:15", "19890513 073215");

        for (final String format : formats) {
            assertEquals(ByteBuffer.wrap(expectedParsedDateTime.getBytes()),
                    FieldParser.parse(format, schema));
        }
    }

    @Test(expected = ParseException.class)
    public void testParseInvalidDateTimeFormat() throws ParseException {
        ColumnSchema schema = ColumnSchema
                .builder(COLUMN_NAME, ColumnType.DATETIME)
                .build();

        FieldParser.parse("1989_05_13_07_32_15", schema);
    }

    @Test
    public void testParseDecimal() throws Exception {
        ColumnSchema schema = ColumnSchema
                .builder(COLUMN_NAME, ColumnType.DECIMAL)
                .setPrecision(5)
                .setScale(2)
                .build();

        assertEquals(ByteBuffer.wrap(Arrays.copyOfRange(Bytes.toBytes(0x807B2DL), 5, 8)),
                FieldParser.parse("123.45", schema));
        assertEquals(ByteBuffer.wrap(Arrays.copyOfRange(Bytes.toBytes(0x7F84D2L), 5, 8)),
                FieldParser.parse("-123.45", schema));
        assertEquals(ByteBuffer.wrap(Arrays.copyOfRange(Bytes.toBytes(0x800000L), 5, 8)),
                FieldParser.parse("000.00", schema));
        assertEquals(ByteBuffer.wrap(Arrays.copyOfRange(Bytes.toBytes(0x800000L), 5, 8)),
                FieldParser.parse("-000.00", schema));
        assertEquals(ByteBuffer.wrap(Arrays.copyOfRange(Bytes.toBytes(0x83E763L), 5, 8)),
                FieldParser.parse("999.99", schema));
        assertEquals(ByteBuffer.wrap(Arrays.copyOfRange(Bytes.toBytes(0x7C189CL), 5, 8)),
                FieldParser.parse("-999.99", schema));

        schema = ColumnSchema
                .builder(COLUMN_NAME, ColumnType.DECIMAL)
                .setPrecision(10)
                .setScale(3)
                .build();

        assertEquals(ByteBuffer.wrap(Arrays.copyOfRange(Bytes.toBytes(0x008012D687037AL), 2, 8)),
                FieldParser.parse("1234567.890", schema));

        assertEquals(ByteBuffer.wrap(Arrays.copyOfRange(Bytes.toBytes(0x008000000501F4L), 2, 8)),
                FieldParser.parse("5.5", schema));
    }

    @Test
    public void testBytesFromDigits() {
        assertEquals(0, FieldParser.bytesFromDigits(0));
        assertEquals(1, FieldParser.bytesFromDigits(1));
        assertEquals(1, FieldParser.bytesFromDigits(2));
        assertEquals(2, FieldParser.bytesFromDigits(3));
        assertEquals(2, FieldParser.bytesFromDigits(4));
        assertEquals(3, FieldParser.bytesFromDigits(5));
        assertEquals(3, FieldParser.bytesFromDigits(6));
        assertEquals(4, FieldParser.bytesFromDigits(7));
        assertEquals(4, FieldParser.bytesFromDigits(8));
        assertEquals(4, FieldParser.bytesFromDigits(9));
        assertEquals(5, FieldParser.bytesFromDigits(10));
        assertEquals(5, FieldParser.bytesFromDigits(11));
        assertEquals(6, FieldParser.bytesFromDigits(12));
        assertEquals(6, FieldParser.bytesFromDigits(13));
        assertEquals(7, FieldParser.bytesFromDigits(14));
        assertEquals(7, FieldParser.bytesFromDigits(15));
        assertEquals(8, FieldParser.bytesFromDigits(16));
        assertEquals(8, FieldParser.bytesFromDigits(17));
        assertEquals(8, FieldParser.bytesFromDigits(18));
        assertEquals(9, FieldParser.bytesFromDigits(19));
        assertEquals(9, FieldParser.bytesFromDigits(20));
    }

    @Test
    public void testParseStringEmptyValue() throws ParseException {
        ColumnSchema schema = ColumnSchema
                .builder(COLUMN_NAME, ColumnType.STRING)
                .setMaxLength(32)
                .build();

        assertEquals(ByteBuffer.wrap(EMPTY_STRING.getBytes(Charsets.UTF_8)),
                FieldParser.parse(EMPTY_STRING, schema));
    }

    @Test
    public void testParseBinaryEmptyValue() throws ParseException {
        ColumnSchema schema = ColumnSchema
                .builder(COLUMN_NAME, ColumnType.BINARY)
                .setMaxLength(32)
                .build();

        assertEquals(ByteBuffer.wrap(EMPTY_STRING.getBytes(Charsets.UTF_8)),
                FieldParser.parse(EMPTY_STRING, schema));
    }
}
