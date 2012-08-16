package com.nearinfinity.mysqlengine;

import org.apache.hadoop.hbase.filter.FilterBase;

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

    public ExactValueFilter() {
        super();
    }

    public ExactValueFilter(byte[] value) {
        this.value = value;
    }

    @Override
    public boolean filterRowKey(byte[] buffer, int offset, int length) {
        byte[] rowKey = ByteBuffer.wrap(buffer, offset, length).array();
        byte[] indexValue = parseValueFromIndexRow(rowKey);
        return !Arrays.equals(value, indexValue);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.write(value);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        dataInput.readFully(value);
    }

    private byte[] parseValueFromIndexRow(byte[] rowKey) {
        return ByteBuffer.wrap(rowKey, 17, value.length).array();
    }
}
