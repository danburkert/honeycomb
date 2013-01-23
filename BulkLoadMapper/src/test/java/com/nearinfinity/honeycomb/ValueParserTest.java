package com.nearinfinity.honeycomb;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.nio.charset.Charset;
import java.text.ParseException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.nearinfinity.honeycomb.hbaseclient.ColumnMetadata;
import com.nearinfinity.honeycomb.hbaseclient.ColumnType;

/**
 * Provides test cases for the {@link ValueParser} class. All test values used
 * for column type data comes from the valid datatype ranges of the database in
 * which the values were received from
 *
 */
public class ValueParserTest {

    private ColumnMetadata metadata;

    @Before
    public void setupTestCase() {
        metadata = new ColumnMetadata();
    }

    @Test(expected = NullPointerException.class)
    public void testParseNullValue() throws ParseException {
        ValueParser.parse(null, metadata);
    }

    @Test(expected = NullPointerException.class)
    public void testParseNullMetadata() throws ParseException {
        ValueParser.parse("foo", null);
    }

    /**
     * Tests a parse request with an empty value and a nullable
     * {@link ColumnMetadata} with a {@link ColumnType} not equal to
     * {@link ColumnType#STRING} or {@link ColumnType#BINARY}
     *
     * @throws ParseException
     */
    @Test
    public void testParseEmptyValueNullableMetadataLongType()
            throws ParseException {
        metadata.setType(ColumnType.LONG);
        metadata.setNullable(true);

        assertNull(ValueParser.parse("", metadata));
    }

    /**
     * Tests a parse request with an empty value and a non-nullable
     * {@link ColumnMetadata} with a {@link ColumnType} not equal to
     * {@link ColumnType#STRING} or {@link ColumnType#BINARY}
     *
     * @throws ParseException
     */
    @Test(expected = IllegalArgumentException.class)
    public void testParseEmptyValueNonNullableMetadataLongType()
            throws ParseException {
        metadata.setType(ColumnType.LONG);
        metadata.setNullable(false);

        assertNull(ValueParser.parse("", metadata));
    }

    @Test
    public void testParseLongZeroValue() throws ParseException {
        metadata.setType(ColumnType.LONG);

        assertArrayEquals(Bytes.toBytes(0x00L),
                ValueParser.parse("0", metadata));
    }

    @Test
    public void testParseLongNegativeValue() throws ParseException {
        metadata.setType(ColumnType.LONG);

        assertArrayEquals(Bytes.toBytes(0xFFFFFFFFFFFFFF85L),
                ValueParser.parse("-123", metadata));
    }

    @Test
    public void testParseLongPositiveValue() throws ParseException {
        metadata.setType(ColumnType.LONG);

        assertArrayEquals(Bytes.toBytes(0x7BL),
                ValueParser.parse("123", metadata));
    }

    @Test
    public void testParseLongMaxValue() throws ParseException {
        metadata.setType(ColumnType.LONG);

        assertArrayEquals(Bytes.toBytes(0x7FFFFFFFFFFFFFFFL),
                ValueParser.parse("9223372036854775807", metadata));
    }

    @Test
    public void testParseLongMinValue() throws ParseException {
        metadata.setType(ColumnType.LONG);

        assertArrayEquals(Bytes.toBytes(0x8000000000000000L),
                ValueParser.parse("-9223372036854775808", metadata));
    }

    @Test
    public void testParseULongZeroValue() throws ParseException {
        metadata.setType(ColumnType.ULONG);

        assertArrayEquals(Bytes.toBytes(0x00L),
                ValueParser.parse("0", metadata));
    }

    @Test
    public void testParseULongArbitraryValue() throws ParseException {
        metadata.setType(ColumnType.ULONG);

        assertArrayEquals(Bytes.toBytes(0x7BL),
                ValueParser.parse("123", metadata));
    }

    @Test
    public void testParseULongMaxValue() throws ParseException {
        metadata.setType(ColumnType.ULONG);

        assertArrayEquals(Bytes.toBytes(0xFFFFFFFFFFFFFFFFL),
                ValueParser.parse("18446744073709551615", metadata));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseULongNegativeInput() throws ParseException {
        metadata.setType(ColumnType.ULONG);

        ValueParser.parse("-123", metadata);
    }

    @Test
    public void testParseDoubleZeroValue() throws ParseException {
        metadata.setType(ColumnType.DOUBLE);

        // Note: These values are all big endian, as per the JVM
        assertArrayEquals(Bytes.toBytes(0x00L),
                ValueParser.parse("0.0", metadata));
    }

    @Test
    public void testParseDoublePositiveValue() throws ParseException {
        metadata.setType(ColumnType.DOUBLE);

        // Note: These values are all big endian, as per the JVM
        assertArrayEquals(Bytes.toBytes(0x40283D70A3D70A3DL),
                ValueParser.parse("12.12", metadata));
    }

    @Test
    public void testParseDoubleNegativeValue() throws ParseException {
        metadata.setType(ColumnType.DOUBLE);

        // Note: These values are all big endian, as per the JVM
        assertArrayEquals(Bytes.toBytes(0xC0283D70A3D70A3DL),
                ValueParser.parse("-12.12", metadata));
    }

    @Test
    public void testParseValidDateFormats() throws ParseException {
        metadata.setType(ColumnType.DATE);

        final String expectedParsedDate = "1989-05-13";

        final List<String> formats = ImmutableList.of(
                expectedParsedDate, "1989.05.13", "1989/05/13", "19890513");

        for (final String format : formats) {
            assertArrayEquals(expectedParsedDate.getBytes(),
                    ValueParser.parse(format, metadata));
        }
    }

    @Test(expected = ParseException.class)
    public void testParseInvalidDateFormat() throws ParseException {
        metadata.setType(ColumnType.DATE);

        ValueParser.parse("1989_05_13", metadata);
    }

    @Test
    public void testParseTime() throws Exception {
        metadata.setType(ColumnType.TIME);

        final String expectedParsedTime = "07:32:15";

        final List<String> formats = ImmutableList.of(
                expectedParsedTime, "073215");

        for (String format : formats) {
            assertArrayEquals(expectedParsedTime.getBytes(),
                    ValueParser.parse(format, metadata));
        }
    }

    @Test(expected = ParseException.class)
    public void testParseInvalidTimeFormat() throws ParseException {
        metadata.setType(ColumnType.TIME);

        ValueParser.parse("07_32_15", metadata);
    }

    @Test
    public void testParseDateTime() throws Exception {
        metadata.setType(ColumnType.DATETIME);

        String[] formats = { "1989-05-13 07:32:15", "1989.05.13 07:32:15",
                "1989/05/13 07:32:15", "19890513 073215" };

        for (String format : formats) {
            assertArrayEquals("1989-05-13 07:32:15".getBytes(),
                    ValueParser.parse(format, metadata));
        }
    }

    @Test
    public void testParseDecimal() throws Exception {
        metadata.setType(ColumnType.DECIMAL);
        metadata.setPrecision(5);
        metadata.setScale(2);

        assertArrayEquals(Arrays.copyOfRange(Bytes.toBytes(0x807B2DL), 5, 8),
                ValueParser.parse("123.45", metadata));
        assertArrayEquals(Arrays.copyOfRange(Bytes.toBytes(0x7F84D2L), 5, 8),
                ValueParser.parse("-123.45", metadata));
        assertArrayEquals(Arrays.copyOfRange(Bytes.toBytes(0x800000L), 5, 8),
                ValueParser.parse("000.00", metadata));
        assertArrayEquals(Arrays.copyOfRange(Bytes.toBytes(0x800000L), 5, 8),
                ValueParser.parse("-000.00", metadata));
        assertArrayEquals(Arrays.copyOfRange(Bytes.toBytes(0x83E763L), 5, 8),
                ValueParser.parse("999.99", metadata));
        assertArrayEquals(Arrays.copyOfRange(Bytes.toBytes(0x7C189CL), 5, 8),
                ValueParser.parse("-999.99", metadata));

        metadata.setPrecision(10);
        metadata.setScale(3);

        assertArrayEquals(
                Arrays.copyOfRange(Bytes.toBytes(0x008012D687037AL), 2, 8),
                ValueParser.parse("1234567.890", metadata));
        assertArrayEquals(
                Arrays.copyOfRange(Bytes.toBytes(0x008000000501F4L), 2, 8),
                ValueParser.parse("5.5", metadata));
    }

    @Test
    public void testBytesFromDigits() {
        assertEquals(0, ValueParser.bytesFromDigits(0));
        assertEquals(1, ValueParser.bytesFromDigits(1));
        assertEquals(1, ValueParser.bytesFromDigits(2));
        assertEquals(2, ValueParser.bytesFromDigits(3));
        assertEquals(2, ValueParser.bytesFromDigits(4));
        assertEquals(3, ValueParser.bytesFromDigits(5));
        assertEquals(3, ValueParser.bytesFromDigits(6));
        assertEquals(4, ValueParser.bytesFromDigits(7));
        assertEquals(4, ValueParser.bytesFromDigits(8));
        assertEquals(4, ValueParser.bytesFromDigits(9));
        assertEquals(5, ValueParser.bytesFromDigits(10));
        assertEquals(5, ValueParser.bytesFromDigits(11));
        assertEquals(6, ValueParser.bytesFromDigits(12));
        assertEquals(6, ValueParser.bytesFromDigits(13));
        assertEquals(7, ValueParser.bytesFromDigits(14));
        assertEquals(7, ValueParser.bytesFromDigits(15));
        assertEquals(8, ValueParser.bytesFromDigits(16));
        assertEquals(8, ValueParser.bytesFromDigits(17));
        assertEquals(8, ValueParser.bytesFromDigits(18));
        assertEquals(9, ValueParser.bytesFromDigits(19));
        assertEquals(9, ValueParser.bytesFromDigits(20));
    }

    @Test
    public void testParseStringEmptyValue() throws ParseException {
        metadata.setType(ColumnType.STRING);

        assertArrayEquals("".getBytes(Charset.forName("UTF-8")),
                ValueParser.parse("", metadata));
    }

    @Test
    public void testParseBinaryEmptyValue() throws ParseException {
        metadata.setType(ColumnType.BINARY);

        assertArrayEquals("".getBytes(Charset.forName("UTF-8")),
                ValueParser.parse("", metadata));
    }
}
