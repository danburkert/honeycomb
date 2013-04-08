package com.nearinfinity.honeycomb.hbase.rowkey;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.UUID;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.nearinfinity.honeycomb.hbase.VarEncoder;
import com.nearinfinity.honeycomb.mysql.Util;
import com.nearinfinity.honeycomb.util.Verify;

/**
 * Super class for index rowkeys
 */
public abstract class IndexRowKey implements RowKey {
    private final byte prefix;
    private final long tableId;
    private final long indexId;
    private final byte[] notNullBytes;
    private final byte[] nullBytes;
    private final UUID uuid;
    private final List<byte[]> records;
    private final SortOrder sortOrder;

    protected IndexRowKey(final long tableId,
                       final long indexId,
                       final List<byte[]> records,
                       final UUID uuid,
                       final byte prefix,
                       final byte[] notNullBytes,
                       final byte[] nullBytes,
                       final SortOrder sortOrder) {
        Verify.isValidId(tableId);
        checkArgument(indexId >= 0, "Index ID must be non-zero.");
        checkNotNull(prefix, "Prefix cannot be null");
        checkNotNull(notNullBytes, "Not null bytes cannot be null");
        checkNotNull(nullBytes, "Null bytes cannot be null");
        this.uuid = uuid;
        this.tableId = tableId;
        this.indexId = indexId;
        this.prefix = prefix;
        this.notNullBytes = notNullBytes;
        this.nullBytes = nullBytes;
        this.records = records;
        this.sortOrder = sortOrder;
    }

    @Override
    public byte[] encode() {
        final byte[] prefixBytes = {prefix};
        final List<byte[]> encodedParts = Lists.newArrayList();
        encodedParts.add(prefixBytes);
        encodedParts.add(VarEncoder.encodeULong(tableId));
        encodedParts.add(VarEncoder.encodeULong(indexId));

        if (records != null) {
            for (final byte[] record : records) {
                if (record == null) {
                    encodedParts.add(nullBytes);
                } else {
                    encodedParts.add(notNullBytes);
                    encodedParts.add(record);
                }
            }
            if (uuid != null) {
                encodedParts.add(Util.UUIDToBytes(uuid));
            }
        }

        return VarEncoder.appendByteArrays(encodedParts);
    }

    @Override
    public byte getPrefix() {
        return prefix;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this.getClass())
                .add("Prefix", String.format("%02X", prefix))
                .add("TableId", tableId)
                .add("IndexId", indexId)
                .add("Records", records == null ? "" : recordValueStrings())
                .add("UUID", uuid == null ? "" : Util.generateHexString(Util.UUIDToBytes(uuid)))
                .toString();
    }

    private List<String> recordValueStrings() {
        final List<String> strings = Lists.newArrayList();

        for (final byte[] bytes : records) {
            strings.add((bytes == null) ? "null" : Util.generateHexString(bytes));
        }

        return strings;
    }

    @Override
    public int compareTo(RowKey o) {
        int typeCompare = getPrefix() - o.getPrefix();
        if (typeCompare != 0) { return typeCompare; }
        IndexRowKey row2 = (IndexRowKey) o;

        int nullOrder = sortOrder == SortOrder.Ascending ? -1 : 1;

        int compare;
        compare = Long.signum(tableId - row2.tableId);
        if (compare != 0) {
            return compare;
        }
        compare = Long.signum(indexId - row2.indexId);
        if (compare != 0) {
            return compare;
        }
        compare = recordsCompare(records, row2.records, nullOrder);
        if (compare != 0) {
            return compare;
        }
        return new Bytes.ByteArrayComparator().compare(
                Util.UUIDToBytes(uuid),
                Util.UUIDToBytes(row2.uuid));
    }

    private static int recordsCompare(List<byte[]> records1, List<byte[]> records2, int nullOrder) {
        byte[] value1, value2;
        int compare;
        if (records1.size() != records2.size()) {
            throw new IllegalArgumentException("Number of records in indices must match.");
        }
        for (int i = 0; i < records1.size(); i++) {
            value1 = records1.get(i);
            value2 = records2.get(i);
            if (value1 == value2) {
                continue;
            }
            if (value1 == null) {
                return nullOrder;
            }
            if (value2 == null) {
                return nullOrder * -1;
            }
            compare = new Bytes.ByteArrayComparator().compare(value1, value2);
            if (compare != 0) {
                return compare;
            }
        }
        return 0;
    }
}
