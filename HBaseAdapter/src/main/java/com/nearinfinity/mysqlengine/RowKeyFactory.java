package com.nearinfinity.mysqlengine;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Created with IntelliJ IDEA.
 * User: jedstrom
 * Date: 8/9/12
 * Time: 9:43 AM
 * To change this template use File | Settings | File Templates.
 */
public class RowKeyFactory {
    public static final byte[] ROOT = ByteBuffer.allocate(7)
                                            .put(RowType.TABLES.getValue())
                                            .put("TABLES".getBytes())
                                            .array();

    public static byte[] buildColumnsKey(long tableId) {
        return ByteBuffer.allocate(9)
                .put(RowType.COLUMNS.getValue())
                .putLong(tableId)
                .array();
    }

    public static byte[] buildDataKey(long tableId, UUID uuid) {
        return ByteBuffer.allocate(25)
                .put(RowType.DATA.getValue())
                .putLong(tableId)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .array();
    }

    public static byte[] buildIndexKey(long tableId, long columnId, byte[] value, UUID uuid) {
        return ByteBuffer.allocate(33 + value.length)
                .put(RowType.INDEX.getValue())
                .putLong(tableId)
                .putLong(columnId)
                .put(value)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .array();
    }
}
