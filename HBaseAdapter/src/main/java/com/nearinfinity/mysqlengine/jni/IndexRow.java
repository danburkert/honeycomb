package com.nearinfinity.mysqlengine.jni;

import com.nearinfinity.hbaseclient.ResultParser;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Created with IntelliJ IDEA.
 * User: jedstrom
 * Date: 8/17/12
 * Time: 10:50 AM
 * To change this template use File | Settings | File Templates.
 */
public class IndexRow {
    private byte[] unireg;
    private byte[] uuid;

    private static final Logger logger = Logger.getLogger(IndexRow.class);

    public IndexRow() {
        this.unireg = null;
        this.uuid = null;
    }

    public IndexRow(byte[] unireg, byte[] uuid) {
        this.unireg = unireg;
        this.uuid = uuid;
    }

    public byte[] getUnireg() {
        return this.unireg;
    }

    public byte[] getUUID() {
        return uuid;
    }

    public void setUnireg(byte[] unireg) {
        this.unireg = unireg;
    }

    public void setUUID(UUID rowUuid) {
        this.uuid = ByteBuffer.allocate(16)
                .putLong(rowUuid.getMostSignificantBits())
                .putLong(rowUuid.getLeastSignificantBits())
                .array();
    }

    public void parseResult(Result result) {
        this.setUnireg(ResultParser.parseUnireg(result));
        this.setUUID(ResultParser.parseUUID(result));
    }
}
