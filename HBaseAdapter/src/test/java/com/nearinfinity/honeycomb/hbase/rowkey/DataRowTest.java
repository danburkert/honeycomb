package com.nearinfinity.honeycomb.hbase.rowkey;

import static org.junit.Assert.assertArrayEquals;

import java.util.UUID;

import org.junit.Test;

import com.nearinfinity.honeycomb.hbase.VarEncoder;
import com.nearinfinity.honeycomb.mysql.Util;

public class DataRowTest {

    private static final long TABLE_ID = 1;
    private static final byte DATA_ROW_PREFIX = 0x06;

    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testConstructDataRowInvalidTableId() {
        final long invalidTableId = -1;
        new DataRow(invalidTableId);
    }

    @Test
    public void testEncodeDataRow() {
        final UUID rowUUID = UUID.randomUUID();
        final DataRow row = new DataRow(TABLE_ID, rowUUID);

        final byte[] expectedEncoding = VarEncoder.appendByteArraysWithPrefix(DATA_ROW_PREFIX,
                                    VarEncoder.encodeULong(TABLE_ID),
                                    Util.UUIDToBytes(rowUUID));

        assertArrayEquals(expectedEncoding, row.encode());
    }

    @Test
    public void testEncodeDataRowNullUUID() {
        final DataRow row = new DataRow(TABLE_ID);

        final byte[] expectedEncoding = VarEncoder.appendByteArraysWithPrefix(DATA_ROW_PREFIX,
                                    VarEncoder.encodeULong(TABLE_ID));

        assertArrayEquals(expectedEncoding, row.encode());
    }
}
