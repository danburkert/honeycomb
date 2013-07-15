package com.nearinfinity.honeycomb.hbase.rowkey;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import com.gotometrics.orderly.RowKey;
import com.nearinfinity.honeycomb.exceptions.RuntimeIOException;

/**
 * Represents a portion of a rowkey that is represented by an Orderly serialized {@link RowKey} type
 */
public class RowKeyValue {
    private final RowKey rowKey;
    private final Object value;

    /**
     *
     * @param rowKey The row key instance type used for serialization
     * @param value The value to serialize
     */
    public RowKeyValue(final RowKey rowKey, final Object value) {
        checkNotNull(rowKey);
        checkNotNull(value);

        this.rowKey = rowKey;
        this.value = value;
    }

    /**
     * Serializes the stored value with the stored {@link RowKey} type
     * @return The serialized value
     */
    public byte[] serialize() {
        try {
            return rowKey.serialize(value);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public RowKey getRowKey() {
        return rowKey;
    }

    public Object getValue() {
        return value;
    }
}
