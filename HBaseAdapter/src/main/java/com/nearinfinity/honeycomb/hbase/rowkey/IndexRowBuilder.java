package com.nearinfinity.honeycomb.hbase.rowkey;

import com.google.common.collect.ImmutableMap;
import com.nearinfinity.honeycomb.mysql.gen.ColumnType;
import org.apache.hadoop.hbase.util.Bytes;

import java.nio.ByteBuffer;
import java.util.*;

public class IndexRowBuilder {
    private static final long INVERT_SIGN_MASK = 0x8000000000000000L;
    private long tableId;
    private long indexId;
    private SortOrder order;
    private Map<String, byte[]> records;
    private Map<String, ColumnType> columnTypes;
    private Collection<String> columnOrder;
    private UUID uuid;

    private IndexRowBuilder() {
    }

    public static IndexRowBuilder newBuilder(long tableId, long indexId) {
        IndexRowBuilder builder = new IndexRowBuilder();
        builder.tableId = tableId;
        builder.indexId = indexId;
        builder.order = SortOrder.Ascending;
        return builder;
    }

    public IndexRowBuilder withSortOrder(SortOrder sortOrder) {
        this.order = sortOrder;
        return this;
    }

    public IndexRowBuilder withUUID(UUID uuid) {
        this.uuid = uuid;
        return this;
    }

    public IndexRowBuilder withRecords(Map<String, byte[]> records, Map<String, ColumnType> columnTypes, Collection<String> columnOrder) {
        this.records = records;
        this.columnTypes = columnTypes;
        this.columnOrder = columnOrder;
        return this;
    }

    public IndexRow build() {
        List<byte[]> sortedRecords = null;
        if (this.records != null) {
            Map<String, byte[]> encodedRow = encodeRow(this.records, this.columnTypes);
            if (order == SortOrder.Descending) {
                encodedRow = reverseRowValues(encodedRow);
            }

            sortedRecords = getValuesInColumnOrder(encodedRow, this.columnOrder);
        }

        if (order == SortOrder.Ascending) {
            return new AscIndexRow(this.tableId, this.indexId, sortedRecords, this.uuid);
        }

        return new DescIndexRow(this.tableId, this.indexId, sortedRecords, this.uuid);
    }

    private static List<byte[]> getValuesInColumnOrder(Map<String, byte[]> records, Collection<String> columns) {
        List<byte[]> sortedRecords = new LinkedList<byte[]>();
        for (String column : columns) {
            sortedRecords.add(records.get(column));
        }
        return sortedRecords;
    }

    private static Map<String, byte[]> reverseRowValues(Map<String, byte[]> row) {
        final ImmutableMap.Builder<String, byte[]> result = ImmutableMap.builder();
        for (Map.Entry<String, byte[]> entry : row.entrySet()) {
            result.put(entry.getKey(), reverseValue(entry.getValue()));
        }
        return result.build();
    }

    private static byte[] reverseValue(byte[] value) {
        ByteBuffer buffer = ByteBuffer.allocate(value.length);

        for (byte aValue : value) {
            buffer.put((byte) (~aValue));
        }

        return buffer.array();
    }

    private static Map<String, byte[]> encodeRow(Map<String, byte[]> rows, Map<String, ColumnType> columnTypes) {
        final ImmutableMap.Builder<String, byte[]> result = ImmutableMap.builder();
        for (Map.Entry<String, byte[]> entry : rows.entrySet()) {
            byte[] encodedValue = encodeValue(entry.getValue(), columnTypes.get(entry.getKey()));
            result.put(entry.getKey(), encodedValue);
        }

        return result.build();
    }

    private static byte[] encodeValue(final byte[] value, final ColumnType columnType) {
        switch (columnType) {
            case LONG:
            case ULONG:
            case TIME: {
                long longValue = ByteBuffer.wrap(value).getLong();
                return Bytes.toBytes(longValue ^ INVERT_SIGN_MASK);
            }
            case DOUBLE: {
                double doubleValue = ByteBuffer.wrap(value).getDouble();
                long longValue = Double.doubleToLongBits(doubleValue);
                if (doubleValue < 0.0) {
                    return Bytes.toBytes(~longValue);
                }

                return Bytes.toBytes(longValue ^ INVERT_SIGN_MASK);
            }
            default:
                return value;
        }
    }

    private static class DescIndexRow extends IndexRow {
        private static final byte PREFIX = 0x08;
        private static final byte[] NOT_NULL_BYTES = {0x00};
        private static final byte[] NULL_BYTES = {0x01};

        public DescIndexRow(long tableId, long indexId,
                            List<byte[]> records, UUID uuid) {
            super(tableId, indexId, records, uuid, PREFIX,
                    NOT_NULL_BYTES, NULL_BYTES);
        }

        @Override
        public SortOrder getSortOrder() {
            return SortOrder.Descending;
        }
    }

    private static class AscIndexRow extends IndexRow {
        private static final byte PREFIX = 0x07;
        private static final byte[] NOT_NULL_BYTES = {0x01};
        private static final byte[] NULL_BYTES = {0x00};

        public AscIndexRow(long tableId, long indexId,
                           List<byte[]> records, UUID uuid) {
            super(tableId, indexId, records, uuid, PREFIX,
                    NOT_NULL_BYTES, NULL_BYTES);
        }

        @Override
        public SortOrder getSortOrder() {
            return SortOrder.Ascending;
        }
    }
}
