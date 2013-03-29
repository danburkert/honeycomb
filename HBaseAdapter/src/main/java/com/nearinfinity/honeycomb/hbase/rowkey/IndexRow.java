package com.nearinfinity.honeycomb.hbase.rowkey;

import com.nearinfinity.honeycomb.hbase.VarEncoder;
import com.nearinfinity.honeycomb.mysql.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

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
                    encodedParts.add(VarEncoder.encodeBytes(record));
                }
            }
        }
        if (uuid != null) {
            encodedParts.add(Util.UUIDToBytes(uuid));
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
        StringBuilder sb = new StringBuilder("[");
        sb.append(String.format("%02X", prefix));
        sb.append("\t");
        sb.append(tableId);
        sb.append("\t");
        sb.append(indexId);
        sb.append("\t");
        sb.append(records == null ? "" : recordValueStrings());
        sb.append("\t");
        sb.append(uuid == null ? "" : Util.generateHexString(Util.UUIDToBytes(uuid)));
        sb.append("]");
        return sb.toString();
    }

    private List<String> recordValueStrings() {
        List<String> strings = new ArrayList<String>();
        for (byte[] bytes : records) {
            strings.add((bytes == null) ? "null" : Util.generateHexString(bytes));
        }
        return strings;
    }
}