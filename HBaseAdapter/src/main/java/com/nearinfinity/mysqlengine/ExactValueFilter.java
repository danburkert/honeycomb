package com.nearinfinity.mysqlengine;

import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 * User: jedstrom
 * Date: 8/16/12
 * Time: 7:35 AM
 * To change this template use File | Settings | File Templates.
 */
public class ExactValueFilter extends FilterBase {

    private byte[] value;

    private static final Logger logger = Logger.getLogger(ExactValueFilter.class);

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
        return wrapAndGet(rowKey, 17, value.length);
    }

    private byte[] wrapAndGet(byte[] array, int offset, int length) {
        ByteBuffer buffer = ByteBuffer.wrap(array);
        buffer.position(offset);
        byte[] ans = new byte[length];
        buffer.get(ans);
        return ans;
    }
}
