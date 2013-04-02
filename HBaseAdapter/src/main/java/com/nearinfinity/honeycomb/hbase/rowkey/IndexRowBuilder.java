package com.nearinfinity.honeycomb.hbase.rowkey;

import com.google.common.collect.Lists;
import com.nearinfinity.honeycomb.mysql.gen.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.gen.IndexSchema;
import org.apache.hadoop.hbase.util.Bytes;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class IndexRowBuilder {
    private static final long INVERT_SIGN_MASK = 0x8000000000000000L;
    private long tableId;
    private long indexId;
    private SortOrder order;
    private IndexSchema schema;
    private Map<String, ColumnSchema> columnSchemas;
    private Map<String, ByteBuffer> records;
    private UUID uuid;

    private IndexRowBuilder() {
    }

    public static IndexRowBuilder newBuilder(long tableId,
                                             long indexId) {
        IndexRowBuilder builder = new IndexRowBuilder();
        builder.tableId = tableId;
        builder.indexId = indexId;

        return builder;
    }

    public IndexRowBuilder withSortOrder(SortOrder order) {
        this.order = order;
        return this;
    }

    public IndexRowBuilder withRecords(Map<String, ByteBuffer> records,
                                       IndexSchema schema,
                                       Map<String, ColumnSchema> columnSchemas) {
        checkNotNull(schema, "Index schema must be set on IndexRowBuilder");
        checkNotNull(columnSchemas, "Column schemas must be set on IndexRowBuilder");
        this.records = records;
        this.schema = schema;
        this.columnSchemas = columnSchemas;
        return this;
    }

    public IndexRowBuilder withUUID(UUID uuid) {
        this.uuid = uuid;
        return this;
    }

    public IndexRow build() {
        checkState(order != null, "Sort order must be set on IndexRowBuilder.");
        List<byte[]> encodedRecords = Lists.newArrayList();
        if (this.records != null) {
            for (String column : schema.getColumns()) {
                ByteBuffer record = records.get(column);
                if (record != null) {
                    byte[] encodedRecord = encodeValue(record,
                            columnSchemas.get(column));
                    encodedRecords.add(order == SortOrder.Ascending
                            ? encodedRecord
                            : reverseValue(encodedRecord));
                } else {
                    encodedRecords.add(null);
                }
            }
        }

        if (order == SortOrder.Ascending) {
            return new AscIndexRow(this.tableId, this.indexId, encodedRecords, this.uuid);
        } else {
            return new DescIndexRow(this.tableId, this.indexId, encodedRecords, this.uuid);
        }
    }

    private static byte[] reverseValue(byte[] value) {
        byte[] reversed = new byte[value.length];
        for (int i = 0; i < value.length; i++) {
            reversed[i] = (byte) (~value[i] & 0xFF);
        }
        return reversed;
    }

    private static byte[] encodeValue(final ByteBuffer value, final ColumnSchema columnSchema) {
        try {
            switch (columnSchema.getType()) {
                case LONG:
                case TIME: {
                    long longValue = value.getLong();
                    return Bytes.toBytes(longValue ^ INVERT_SIGN_MASK);
                }
                case DOUBLE: {
                    double doubleValue = value.getDouble();
                    long longValue = Double.doubleToLongBits(doubleValue);
                    if (doubleValue < 0.0) {
                        return Bytes.toBytes(~longValue);
                    }

                    return Bytes.toBytes(longValue ^ INVERT_SIGN_MASK);
                }
                case BINARY:
                case STRING: {
                    return Arrays.copyOf(value.array(), columnSchema.getMaxLength());
                }
                default:
                    return value.array();
            }
        } finally {
            value.rewind(); // rewind the ByteBuffer's index pointer
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
