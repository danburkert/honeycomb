package com.nearinfinity.hbaseclient.filter;

import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class UUIDFilter extends FilterBase {

    private byte[] value;

    public UUIDFilter() {
        this.value = null;
    }

    public UUIDFilter(UUID uuid) {
        this.value = ByteBuffer.allocate(16)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .array();
    }

    @Override
    public boolean filterRowKey(byte[] buffer, int offset, int length) {
        byte[] rowKey = wrapAndGet(buffer, offset, length);
        byte[] uuid = parseUUIDFromKey(rowKey);

        return !Arrays.equals(uuid, this.value);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        Bytes.writeByteArray(dataOutput, this.value);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.value = Bytes.readByteArray(dataInput);
    }

    private byte[] parseUUIDFromKey(byte[] rowKey) {
        return wrapAndGet(rowKey, rowKey.length - 16, 16);
    }

    private byte[] wrapAndGet(byte[] array, int offset, int length) {
        ByteBuffer buffer = ByteBuffer.wrap(array);
        buffer.position(offset);
        byte[] ans = new byte[length];
        buffer.get(ans);
        return ans;
    }
}
