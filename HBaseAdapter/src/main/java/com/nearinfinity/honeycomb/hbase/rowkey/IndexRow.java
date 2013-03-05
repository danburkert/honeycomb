package com.nearinfinity.honeycomb.hbase.rowkey;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.nearinfinity.honeycomb.hbase.RowKey;
import com.nearinfinity.honeycomb.hbase.VarEncoder;
import com.nearinfinity.honeycomb.hbaseclient.Constants;
import com.nearinfinity.honeycomb.mysql.Util;

public class IndexRow implements RowKey {
    private final byte prefix;
    private final long tableId;
    private UUID uuid;
    private final List<Long> columnIds;
    private Map<Long, byte[]> records;
    private final byte[] notNullBytes;
    private final byte[] nullBytes;

    protected IndexRow(long tableId,
                    List<Long> columnIds,
                    byte prefix,
                    byte[] notNullBytes,
                    byte[] nullBytes) {
        checkArgument(tableId >= 0, "Table ID must be non-zero.");
        checkArgument(columnIds.size() >= 1
                && columnIds.size() <= Constants.KEY_PART_COUNT,
                "There must be between 1 and %s columns in an IndexRow.",
                Constants.KEY_PART_COUNT);
        this.tableId = tableId;
        this.columnIds = columnIds;
        records = null;
        uuid = null;
        this.prefix = prefix;
        this.notNullBytes = notNullBytes;
        this.nullBytes = nullBytes;
    }

    protected IndexRow(long tableId,
                    List<Long> columnIds,
                    Map<Long, byte[]> records,
                    byte prefix,
                    byte[] notNullBytes,
                    byte[] nullBytes) {
        this(tableId, columnIds, prefix, notNullBytes, nullBytes);
        checkNotNull(records, "Records may not be null.");
        this.records = records;
    }

    protected IndexRow(long tableId,
                    List<Long> columnIds,
                    Map<Long, byte[]> records,
                    UUID uuid,
                    byte prefix,
                    byte[] notNullBytes,
                    byte[] nullBytes) {
        this(tableId, columnIds, records, prefix, notNullBytes, nullBytes);
        checkNotNull(uuid, "UUID may not be null.");
        this.uuid = uuid;
    }

    @Override
    public byte[] encode() {
        byte[] prefixBytes = {prefix};
        List<byte[]> encodedParts = new ArrayList<byte[]>();
        encodedParts.add(prefixBytes);
        encodedParts.add(VarEncoder.encodeULong(tableId));
        for (Long columnId : columnIds) {
            encodedParts.add(VarEncoder.encodeULong(columnId));
        }
        if (records != null) {
            for (Long columnId : columnIds) {
                byte[] recordValue = records.get(columnId);
                if (recordValue == null) {
                    encodedParts.add(nullBytes);
                } else {
                    encodedParts.add(notNullBytes);
                    encodedParts.add(VarEncoder.encodeBytes(recordValue));
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

    public List<Long> getColumnIds() {
        return columnIds;
    }

    public Map<Long, byte[]> getRecords() {
        return records;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[");
        sb.append(String.format("%02X", prefix));
        sb.append("\t");
        sb.append(tableId);
        sb.append("\t");
        sb.append(columnIds == null ? "" : columnIds);
        sb.append("\t");
        sb.append(records == null ? "" : recordValueStrings());
        sb.append("\t");
        sb.append(uuid == null ? "" : Util.generateHexString(Util.UUIDToBytes(uuid)));
        sb.append("]");
        return sb.toString();
    }

    private List<String> recordValueStrings() {
        List<String> strings = new ArrayList<String>();
        for (byte[] bytes : recordValues()) {
            strings.add((bytes == null) ? "null" : Util.generateHexString(bytes));
        }
        return strings;
    }

    private List<byte[]> recordValues() {
        List<byte[]> recordValues = new ArrayList<byte[]>();
        for (Long columnId : columnIds) {
            recordValues.add(records.get(columnId));
        }
        return recordValues;
    }
}
