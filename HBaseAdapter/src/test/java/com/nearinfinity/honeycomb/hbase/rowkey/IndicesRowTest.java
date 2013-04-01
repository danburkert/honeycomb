package com.nearinfinity.honeycomb.hbase.rowkey;

import static org.junit.Assert.assertArrayEquals;

import org.junit.Test;

import com.nearinfinity.honeycomb.hbase.VarEncoder;

public class IndicesRowTest {
    private static final long TABLE_ID = 1;
    private static final byte INDICES_ROW_PREFIX = 0x02;

    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testConstructIndicesRowInvalidTableId() {
        final long invalidTableId = -1;
        new IndicesRow(invalidTableId);
    }

    @Test
    public void testEncodeIndicesRow() {
        final IndicesRow row = new IndicesRow(TABLE_ID);

        final byte[] expectedEncoding = VarEncoder.appendByteArraysWithPrefix(INDICES_ROW_PREFIX,
                                    VarEncoder.encodeULong(TABLE_ID));

        assertArrayEquals(expectedEncoding, row.encode());
    }
}
