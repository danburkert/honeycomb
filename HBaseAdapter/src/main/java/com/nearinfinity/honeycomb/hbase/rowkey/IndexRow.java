package com.nearinfinity.honeycomb.hbase.rowkey;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.UUID;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.nearinfinity.honeycomb.hbase.VarEncoder;
import com.nearinfinity.honeycomb.mysql.Util;
import com.nearinfinity.honeycomb.mysql.Verify;

/**
 * Representation of the rowkey used to define the indexed column details for data row content
 */
public abstract class IndexRow implements RowKey {
    private final byte prefix;
    private final long tableId;
    private final long indexId;
    private final byte[] notNullBytes;
    private final byte[] nullBytes;
    private final UUID uuid;
    private final List<byte[]> records;

    protected IndexRow(final long tableId,
                       final long indexId,
                       final List<byte[]> records,
                       final UUID uuid,
                       final byte prefix,
                       final byte[] notNullBytes,
                       final byte[] nullBytes) {
        Verify.isValidTableId(tableId);
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
}