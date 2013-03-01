package com.nearinfinity.honeycomb.hbase.rowkey;

import com.google.common.base.Preconditions;
import com.nearinfinity.honeycomb.hbase.RowKey;
import com.nearinfinity.honeycomb.hbase.VarEncoder;
import com.nearinfinity.honeycomb.mysql.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class IndexRow implements RowKey {
    private byte prefix;
    private long tableId;
    private UUID uuid;
    private List<Long> columnIds;
    private Map<Long, byte[]> records;
    private byte[] notNullBytes;
    private byte[] nullBytes;

    public IndexRow(long tableId,
                    UUID uuid,
                    List<Long> columnIds,
                    Map<Long, byte[]> records,
                    byte prefix,
                    byte[] notNullBytes,
                    byte[] nullBytes) {
        Preconditions.checkArgument(columnIds.size() >= 1 && columnIds.size() <= 4,
                "There must be between 1 and 4 columns in an IndexRow.");
        this.tableId = tableId;
        this.uuid = uuid;
        this.columnIds = columnIds;
        this.records = records;
        this.prefix = prefix;
        this.notNullBytes = notNullBytes;
        this.nullBytes = nullBytes;
    }

    public byte[] encode() {
        byte[] prefixBytes = { prefix };
        List<byte[]> encodedParts = new ArrayList<byte[]>();
        encodedParts.add(prefixBytes);
        encodedParts.add(VarEncoder.encodeULong(tableId));
        for(Long columnId : columnIds) {
            encodedParts.add(VarEncoder.encodeULong(columnId));
        }
        for(Long columnId : columnIds) {
            byte[] recordValue = records.get(columnId);
            if (recordValue == null) { encodedParts.add(nullBytes); }
            else {
                encodedParts.add(notNullBytes);
                encodedParts.add(VarEncoder.encodeBytes(recordValue));
            }
        }
        encodedParts.add(Util.UUIDToBytes(uuid));
        return VarEncoder.appendByteArrays(encodedParts);
    }

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
        sb.append(columnIds);
        sb.append("\t");
        sb.append(recordValueStrings());
        sb.append("\t");
        sb.append(Util.generateHexString(Util.UUIDToBytes(uuid)));
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
