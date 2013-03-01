package com.nearinfinity.honeycomb.hbase.rowkey;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class DescIndexRow extends IndexRow {
    private static final byte PREFIX = 0x06;
    private static final byte[] NOT_NULL_BYTES = {0x00};
    private static final byte[] NULL_BYTES = {0x01};

    public DescIndexRow(long tableId, UUID uuid, List<Long> columnIds,
                       Map<Long, byte[]> records) {
        super(tableId, uuid, columnIds, records, PREFIX,
                NOT_NULL_BYTES, NULL_BYTES);
    }
}