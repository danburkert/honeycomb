package com.nearinfinity.honeycomb.hbase.rowkey;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Objects;


/**
 * Representation of a rowkey that contains only the row prefix identifier
 */
public abstract class PrefixRow implements RowKey {
    private final byte[] rowKey;

    /**
     * Creates a prefix rowkey with the provided rowkey content
     *
     * @param rowKey The rowkey content that this row represents, not null or empty
     */
    public PrefixRow(final byte[] rowKey) {
        checkNotNull(rowKey, "The rowkey is invalid");
        checkArgument(rowKey.length > 0, "The rowkey cannot be empty");
        this.rowKey = rowKey;
    }

    @Override
    public byte[] encode() {
        return rowKey;
    }

    @Override
    public byte getPrefix() {
        return rowKey[0];
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this.getClass())
                .add("Prefix", String.format("%02X", getPrefix()))
                .toString();
    }
}