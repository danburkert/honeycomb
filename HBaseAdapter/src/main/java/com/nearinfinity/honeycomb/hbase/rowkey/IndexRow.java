package com.nearinfinity.honeycomb.hbase.rowkey;

import com.google.common.base.Objects;
import com.nearinfinity.honeycomb.hbase.VarEncoder;
import com.nearinfinity.honeycomb.mysql.Util;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Super class for index rowkeys
 */
public abstract class IndexRow implements RowKey {
    private final byte prefix;
    private final long tableId;
    private final long indexId;
    private final byte[] notNullBytes;
    private final byte[] nullBytes;
    private UUID uuid;
    private List<byte[]> records;

    protected IndexRow(long tableId,
                       long indexId,
                       List<byte[]> records,
                       UUID uuid,
                       byte prefix,
                       byte[] notNullBytes,
                       byte[] nullBytes) {
        checkArgument(tableId >= 0, "Table ID must be non-zero.");
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
    }

    @Override
    public byte[] encode() {
        byte[] prefixBytes = {prefix};
        List<byte[]> encodedParts = new ArrayList<byte[]>();
        encodedParts.add(prefixBytes);
        encodedParts.add(VarEncoder.encodeULong(tableId));
        encodedParts.add(VarEncoder.encodeULong(indexId));

        if (records != null) {
            for (byte[] record : records) {
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

    public long getTableId() {
        return tableId;
    }

    public UUID getUuid() {
        return uuid;
    }

    public long getIndexId() {
        return indexId;
    }

    public List<byte[]> getRecords() {
        return records;
    }

    public abstract SortOrder getSortOrder();

    @Override
    public String toString() {
        return Objects.toStringHelper(this.getClass())
                .add("Prefix", String.format("%02X", prefix))
                .add("TableId", tableId)
                .add("IndexId", indexId)
                .add("Records", recordValueStrings())
                .add("UUID", uuid == null ? "" : Util.generateHexString(Util.UUIDToBytes(uuid)))
                .toString();
    }

    private List<String> recordValueStrings() {
        List<String> strings = new ArrayList<String>();
        for (byte[] bytes : records) {
            strings.add((bytes == null) ? "null" : Util.generateHexString(bytes));
        }
        return strings;
    }

    @Override
    public int compareTo(RowKey o) {
        int typeCompare = getPrefix() - o.getPrefix();
        if (typeCompare != 0) { return typeCompare; }
        IndexRow row2 = (IndexRow) o;

        List<byte[]> records1 = getRecords();
        List<byte[]> records2 = row2.getRecords();
        int nullOrder = getSortOrder() == SortOrder.Ascending ? -1 : 1;

        int compare;
        compare = Long.signum(getTableId() - row2.getTableId());
        if (compare != 0) {
            return compare;
        }
        compare = Long.signum(getIndexId() - row2.getIndexId());
        if (compare != 0) {
            return compare;
        }
        compare = recordsCompare(getRecords(), row2.getRecords(), nullOrder);
        if (compare != 0) {
            return compare;
        }
        return new Bytes.ByteArrayComparator().compare(
                Util.UUIDToBytes(getUuid()),
                Util.UUIDToBytes(row2.getUuid()));
    }

    private int recordsCompare(List<byte[]> records1, List<byte[]> records2, int nullOrder) {
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