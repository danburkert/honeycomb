package com.nearinfinity.honeycomb.hbase.rowkey;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class DescIndexRow extends IndexRow {
    private static final byte PREFIX = 0x08;
    private static final byte[] NOT_NULL_BYTES = {0x00};
    private static final byte[] NULL_BYTES = {0x01};

    public DescIndexRow(long tableId, List<Long> columnIds) {
        super(tableId, columnIds, PREFIX, NOT_NULL_BYTES, NULL_BYTES);
    }

    public DescIndexRow(long tableId, List<Long> columnIds,
                        Map<Long, byte[]> records) {
        super(tableId, columnIds, records, PREFIX, NOT_NULL_BYTES, NULL_BYTES);
    }

    public DescIndexRow(long tableId, List<Long> columnIds,
                        Map<Long, byte[]> records, UUID uuid) {
        super(tableId, columnIds, records, uuid, PREFIX,
                NOT_NULL_BYTES, NULL_BYTES);
    }
}