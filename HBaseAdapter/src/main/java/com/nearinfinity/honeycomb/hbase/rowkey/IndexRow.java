package com.nearinfinity.honeycomb.hbase.rowkey;

import com.nearinfinity.honeycomb.hbase.RowKey;
import com.nearinfinity.honeycomb.hbase.VarEncoder;
import com.nearinfinity.honeycomb.mysql.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
                       byte prefix,
                       byte[] notNullBytes,
                       byte[] nullBytes) {
        checkArgument(tableId >= 0, "Table ID must be non-zero.");
        this.tableId = tableId;
        this.indexId = indexId;
        records = null;
        uuid = null;
        this.prefix = prefix;
        this.notNullBytes = notNullBytes;
        this.nullBytes = nullBytes;
    }

    protected IndexRow(long tableId,
                       long indexId,
                       List<byte[]> records,
                       byte prefix,
                       byte[] notNullBytes,
                       byte[] nullBytes) {
        this(tableId, indexId, prefix, notNullBytes, nullBytes);
        checkNotNull(records, "Records may not be null.");
        this.records = records;
    }

    protected IndexRow(long tableId,
                       long indexId,
                       List<byte[]> records,
                       UUID uuid,
                       byte prefix,
                       byte[] notNullBytes,
                       byte[] nullBytes) {
        this(tableId, indexId, records, prefix, notNullBytes, nullBytes);
        checkNotNull(uuid, "UUID may not be null.");
        this.uuid = uuid;
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

    public List<Long> getColumnIds() {
        return null;
    }

    public Map<Long, byte[]> getRecords() {
        return null;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[");
        sb.append(String.format("%02X", prefix));
        sb.append("\t");
        sb.append(tableId);
        sb.append("\t");
        sb.append(indexId);
        sb.append("\t");
        //sb.append(records == null ? "" : recordValueStrings());
        sb.append("\t");
        sb.append(uuid == null ? "" : Util.generateHexString(Util.UUIDToBytes(uuid)));
        sb.append("]");
        return sb.toString();
    }

    //TODO: Re-implement to string
//    private List<String> recordValueStrings() {
//        List<String> strings = new ArrayList<String>();
//        for (byte[] bytes : recordValues()) {
//            strings.add((bytes == null) ? "null" : Util.generateHexString(bytes));
//        }
//        return strings;
//    }
//
//    private List<byte[]> recordValues() {
//        List<byte[]> recordValues = new ArrayList<byte[]>();
//        for (Long columnId : columnIds) {
//            recordValues.add(records.get(columnId));
//        }
//        return recordValues;
//    }
}
