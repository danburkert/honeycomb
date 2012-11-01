package com.nearinfinity.bulkloader;

import com.nearinfinity.hbaseclient.ColumnMetadata;
import com.nearinfinity.hbaseclient.ColumnType;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class ValueParserTest {
    @Test
    public void testParseLong() throws Exception {
        ColumnMetadata m = new ColumnMetadata();
        m.setType(ColumnType.LONG);

        Assert.assertArrayEquals(
                Bytes.toBytes(0x00l),
                ValueParser.parse("00", m)
        );

        Assert.assertArrayEquals(
                Bytes.toBytes(0xFFFFFFFFFFFFFF85l),
                ValueParser.parse("-123", m)
        );

        Assert.assertArrayEquals(
                Bytes.toBytes(0x7Bl),
                ValueParser.parse("123", m)
        );

        Assert.assertArrayEquals(
                Bytes.toBytes(0x7FFFFFFFFFFFFFFFl),
                ValueParser.parse("9223372036854775807", m)
        );

        Assert.assertArrayEquals(
                Bytes.toBytes(0x8000000000000000l),
                ValueParser.parse("-9223372036854775808", m)
        );
    }
    @Test
    public void testParseULong() throws Exception {
        ColumnMetadata m = new ColumnMetadata();
        m.setType(ColumnType.ULONG);

        Assert.assertArrayEquals(
                Bytes.toBytes(0x00l),
                ValueParser.parse("00", m)
        );

        Assert.assertArrayEquals(
                Bytes.toBytes(0x7Bl),
                ValueParser.parse("123", m)
        );

        Assert.assertArrayEquals(
                Bytes.toBytes(0x7FFFFFFFFFFFFFFFl),
                ValueParser.parse("9223372036854775807", m)
        );

        Assert.assertArrayEquals(
                Bytes.toBytes(0x8000000000000000l),
                ValueParser.parse("9223372036854775808", m)
        );

        Assert.assertArrayEquals(
                Bytes.toBytes(0xFFFFFFFFFFFFFFFFl),
                ValueParser.parse("18446744073709551615", m)
        );
    }
    @Test(expected=Exception.class)
    public void testParseULongNegativeInput() throws Exception {
        ColumnMetadata m = new ColumnMetadata();
        m.setType(ColumnType.ULONG);
        ValueParser.parse("-123", m);
    }
    @Test
    public void testParseDouble() throws Exception {
        ColumnMetadata m = new ColumnMetadata();
        m.setType(ColumnType.DOUBLE);
        // Note:  These values are all big endian, as per the JVM
        Assert.assertArrayEquals(
                Bytes.toBytes(0x00l),
                ValueParser.parse("0.0", m)
        );

        Assert.assertArrayEquals(
                Bytes.toBytes(0x40283d70a3d70a3dl),
                ValueParser.parse("12.12", m)
        );

        Assert.assertArrayEquals(
                Bytes.toBytes(0xc0283d70a3d70a3dl),
                ValueParser.parse("-12.12", m)
        );
    }
    @Test
    public void testParseDate() throws Exception {
        ColumnMetadata m = new ColumnMetadata();
        m.setType(ColumnType.DATE);
        String[] formats = {
                "1989-05-13"
              , "1989.05.13"
              , "1989/05/13"
              , "19890513"
        };

        for (String format : formats) {
            Assert.assertArrayEquals(
                    "1989-05-13".getBytes(),
                    ValueParser.parse(format, m)
            );
        }
    }
    @Test
    public void testParseTime() throws Exception {
        ColumnMetadata m = new ColumnMetadata();
        m.setType(ColumnType.TIME);
        String[] formats = {
                "07:32:15"
              , "073215"
        };

        for (String format : formats) {
            Assert.assertArrayEquals(
                    "07:32:15".getBytes(),
                    ValueParser.parse(format, m)
            );
        }
    }
    @Test
    public void testParseDateTime() throws Exception {
        ColumnMetadata m = new ColumnMetadata();
        m.setType(ColumnType.DATETIME);
        String[] formats = {
                "1989-05-13 07:32:15"
              , "1989.05.13 07:32:15"
              , "1989/05/13 07:32:15"
              , "19890513 073215"
        };

        for (String format : formats) {
            Assert.assertArrayEquals(
                    "1989-05-13 07:32:15".getBytes(),
                    ValueParser.parse(format, m)
            );
        }
    }
    @Test
    public void testParseDecimal() throws Exception {
        ColumnMetadata m = new ColumnMetadata();
        m.setType(ColumnType.DECIMAL);
        m.setPrecision(5);
        m.setScale(2);
        Assert.assertArrayEquals(
                Arrays.copyOfRange(Bytes.toBytes(0x807b2dl), 5, 8),
                ValueParser.parse("123.45", m)
        );
        Assert.assertArrayEquals(
                Arrays.copyOfRange(Bytes.toBytes(0x7f84d2l), 5, 8),
                ValueParser.parse("-123.45", m)
        );
        Assert.assertArrayEquals(
                Arrays.copyOfRange(Bytes.toBytes(0x800000l), 5, 8),
                ValueParser.parse("000.00", m)
        );
        Assert.assertArrayEquals(
                Arrays.copyOfRange(Bytes.toBytes(0x800000l), 5, 8),
                ValueParser.parse("-000.00", m)
        );
        Assert.assertArrayEquals(
                Arrays.copyOfRange(Bytes.toBytes(0x83e763l), 5, 8),
                ValueParser.parse("999.99", m)
        );
        Assert.assertArrayEquals(
                Arrays.copyOfRange(Bytes.toBytes(0x7c189cl), 5, 8),
                ValueParser.parse("-999.99", m)
        );

        m.setPrecision(10);
        m.setScale(3);
        Assert.assertArrayEquals(
                Arrays.copyOfRange(Bytes.toBytes(0x008012d687037al), 2, 8),
                ValueParser.parse("1234567.890", m)
        );
        Assert.assertArrayEquals(
                Arrays.copyOfRange(Bytes.toBytes(0x008000000501f4l), 2, 8),
                ValueParser.parse("5.5", m)
        );
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
