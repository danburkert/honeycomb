package com.nearinfinity.hbaseclient.filter;

import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class ExactValueFilter extends FilterBase {

    private byte[] value;

    public ExactValueFilter() {
        super();
    }

    public ExactValueFilter(byte[] value) {
        this.value = value;
    }

    @Override
    public boolean filterRowKey(byte[] buffer, int offset, int length) {
        byte[] rowKey = wrapAndGet(buffer, offset, length);
        byte[] indexValue = parseValueFromIndexRow(rowKey);

        return !Arrays.equals(value, indexValue);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        Bytes.writeByteArray(dataOutput, this.value);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.value = Bytes.readByteArray(dataInput);
    }

    private byte[] parseValueFromIndexRow(byte[] rowKey) {
        //Get the row minus the last 16 bytes (UUID) minus the first 17 bytes (prefix)
        return wrapAndGet(rowKey, 17, rowKey.length - 33);
    }

    private byte[] wrapAndGet(byte[] array, int offset, int length) {
        ByteBuffer buffer = ByteBuffer.wrap(array);
        buffer.position(offset);
        byte[] ans = new byte[length];
        buffer.get(ans);
        return ans;
    }
}
