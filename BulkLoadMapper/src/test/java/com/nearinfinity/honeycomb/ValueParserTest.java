package com.nearinfinity.honeycomb;

import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.nearinfinity.honeycomb.hbaseclient.ColumnMetadata;
import com.nearinfinity.honeycomb.hbaseclient.ColumnType;

public class ValueParserTest {

    private ColumnMetadata metadata;

    @Before
    public void setupTestCase() {
        metadata = new ColumnMetadata();
    }

    @Test
    public void testParseLong() throws Exception {
        metadata.setType(ColumnType.LONG);

        Assert.assertArrayEquals(Bytes.toBytes(0x00L),
                ValueParser.parse("00", metadata));

        Assert.assertArrayEquals(Bytes.toBytes(0xFFFFFFFFFFFFFF85L),
                ValueParser.parse("-123", metadata));

        Assert.assertArrayEquals(Bytes.toBytes(0x7BL),
                ValueParser.parse("123", metadata));

        Assert.assertArrayEquals(Bytes.toBytes(0x7FFFFFFFFFFFFFFFL),
                ValueParser.parse("9223372036854775807", metadata));

        Assert.assertArrayEquals(Bytes.toBytes(0x8000000000000000L),
                ValueParser.parse("-9223372036854775808", metadata));
    }

    @Test
    public void testParseULong() throws Exception {
        metadata.setType(ColumnType.ULONG);

        Assert.assertArrayEquals(Bytes.toBytes(0x00L),
                ValueParser.parse("00", metadata));

        Assert.assertArrayEquals(Bytes.toBytes(0x7BL),
                ValueParser.parse("123", metadata));

        Assert.assertArrayEquals(Bytes.toBytes(0x7FFFFFFFFFFFFFFFL),
                ValueParser.parse("9223372036854775807", metadata));

        Assert.assertArrayEquals(Bytes.toBytes(0x8000000000000000L),
                ValueParser.parse("9223372036854775808", metadata));

        Assert.assertArrayEquals(Bytes.toBytes(0xFFFFFFFFFFFFFFFFL),
                ValueParser.parse("18446744073709551615", metadata));
    }

    @Test(expected = Exception.class)
    public void testParseULongNegativeInput() throws Exception {
        metadata.setType(ColumnType.ULONG);

        ValueParser.parse("-123", metadata);
    }

    @Test
    public void testParseDouble() throws Exception {
        metadata.setType(ColumnType.DOUBLE);

        // Note: These values are all big endian, as per the JVM
        Assert.assertArrayEquals(Bytes.toBytes(0x00L),
                ValueParser.parse("0.0", metadata));

        Assert.assertArrayEquals(Bytes.toBytes(0x40283D70A3D70A3DL),
                ValueParser.parse("12.12", metadata));

        Assert.assertArrayEquals(Bytes.toBytes(0xC0283D70A3D70A3DL),
                ValueParser.parse("-12.12", metadata));
    }

    @Test
    public void testParseDate() throws Exception {
        metadata.setType(ColumnType.DATE);

        String[] formats = { "1989-05-13", "1989.05.13", "1989/05/13",
                "19890513" };

        for (String format : formats) {
            Assert.assertArrayEquals("1989-05-13".getBytes(),
                    ValueParser.parse(format, metadata));
        }
    }

    @Test
    public void testParseTime() throws Exception {
        metadata.setType(ColumnType.TIME);

        String[] formats = { "07:32:15", "073215" };

        for (String format : formats) {
            Assert.assertArrayEquals("07:32:15".getBytes(),
                    ValueParser.parse(format, metadata));
        }
    }

    @Test
    public void testParseDateTime() throws Exception {
        metadata.setType(ColumnType.DATETIME);

        String[] formats = { "1989-05-13 07:32:15", "1989.05.13 07:32:15",
                "1989/05/13 07:32:15", "19890513 073215" };

        for (String format : formats) {
            Assert.assertArrayEquals("1989-05-13 07:32:15".getBytes(),
                    ValueParser.parse(format, metadata));
        }
    }

    @Test
    public void testParseDecimal() throws Exception {
        metadata.setType(ColumnType.DECIMAL);
        metadata.setPrecision(5);
        metadata.setScale(2);

        Assert.assertArrayEquals(
                Arrays.copyOfRange(Bytes.toBytes(0x807B2DL), 5, 8),
                ValueParser.parse("123.45", metadata));
        Assert.assertArrayEquals(
                Arrays.copyOfRange(Bytes.toBytes(0x7F84D2L), 5, 8),
                ValueParser.parse("-123.45", metadata));
        Assert.assertArrayEquals(
                Arrays.copyOfRange(Bytes.toBytes(0x800000L), 5, 8),
                ValueParser.parse("000.00", metadata));
        Assert.assertArrayEquals(
                Arrays.copyOfRange(Bytes.toBytes(0x800000L), 5, 8),
                ValueParser.parse("-000.00", metadata));
        Assert.assertArrayEquals(
                Arrays.copyOfRange(Bytes.toBytes(0x83E763L), 5, 8),
                ValueParser.parse("999.99", metadata));
        Assert.assertArrayEquals(
                Arrays.copyOfRange(Bytes.toBytes(0x7C189CL), 5, 8),
                ValueParser.parse("-999.99", metadata));

        metadata.setPrecision(10);
        metadata.setScale(3);

        Assert.assertArrayEquals(
                Arrays.copyOfRange(Bytes.toBytes(0x008012D687037AL), 2, 8),
                ValueParser.parse("1234567.890", metadata));
        Assert.assertArrayEquals(
                Arrays.copyOfRange(Bytes.toBytes(0x008000000501F4L), 2, 8),
                ValueParser.parse("5.5", metadata));
    }

    @Test
    public void testBytesFromDigits() {
        Assert.assertEquals(0, ValueParser.bytesFromDigits(0));
        Assert.assertEquals(1, ValueParser.bytesFromDigits(1));
        Assert.assertEquals(1, ValueParser.bytesFromDigits(2));
        Assert.assertEquals(2, ValueParser.bytesFromDigits(3));
        Assert.assertEquals(2, ValueParser.bytesFromDigits(4));
        Assert.assertEquals(3, ValueParser.bytesFromDigits(5));
        Assert.assertEquals(3, ValueParser.bytesFromDigits(6));
        Assert.assertEquals(4, ValueParser.bytesFromDigits(7));
        Assert.assertEquals(4, ValueParser.bytesFromDigits(8));
        Assert.assertEquals(4, ValueParser.bytesFromDigits(9));
        Assert.assertEquals(5, ValueParser.bytesFromDigits(10));
        Assert.assertEquals(5, ValueParser.bytesFromDigits(11));
        Assert.assertEquals(6, ValueParser.bytesFromDigits(12));
        Assert.assertEquals(6, ValueParser.bytesFromDigits(13));
        Assert.assertEquals(7, ValueParser.bytesFromDigits(14));
        Assert.assertEquals(7, ValueParser.bytesFromDigits(15));
        Assert.assertEquals(8, ValueParser.bytesFromDigits(16));
        Assert.assertEquals(8, ValueParser.bytesFromDigits(17));
        Assert.assertEquals(8, ValueParser.bytesFromDigits(18));
        Assert.assertEquals(9, ValueParser.bytesFromDigits(19));
        Assert.assertEquals(9, ValueParser.bytesFromDigits(20));
    }
}
