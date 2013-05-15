package com.nearinfinity.honeycomb.hbase.rowkey;

import static org.junit.Assert.assertArrayEquals;

import org.junit.Test;

import com.nearinfinity.honeycomb.hbase.VarEncoder;

public class ColumnsRowKeyTest {
    private static final long TABLE_ID = 1;
    private static final byte COLUMNS_ROW_PREFIX = 0x01;

    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testConstructColumnsRowInvalidTableId() {
        final long invalidTableId = -1;
        new ColumnsRowKey(invalidTableId);
    }

    @Test
    public void testEncodeColumnsRow() {
        final ColumnsRowKey row = new ColumnsRowKey(TABLE_ID);

        final byte[] expectedEncoding = VarEncoder.appendByteArraysWithPrefix(COLUMNS_ROW_PREFIX,
                                    VarEncoder.encodeULong(TABLE_ID));

        assertArrayEquals(expectedEncoding, row.encode());
    }
}
