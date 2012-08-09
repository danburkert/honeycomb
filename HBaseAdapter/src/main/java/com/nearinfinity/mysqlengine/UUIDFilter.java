package com.nearinfinity.mysqlengine;

import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * Created with IntelliJ IDEA.
 * User: jedstrom
 * Date: 8/9/12
 * Time: 4:05 PM
 * To change this template use File | Settings | File Templates.
 */
public class UUIDFilter extends FilterBase {

    private Set<UUID> uuids = new HashSet<UUID>();
    byte[] suffix = null;

    public UUIDFilter(Set<UUID> uuids) {
        this.uuids = uuids;
    }

    @Override
    public boolean filterRowKey(byte[] buffer, int offset, int length) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, offset + length - 16, 16);
        UUID rowUUID = new UUID(byteBuffer.getLong(), byteBuffer.getLong());
        return !uuids.contains(rowUUID);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        Bytes.writeByteArray(dataOutput, this.suffix);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.suffix = Bytes.readByteArray(dataInput);
    }
}
