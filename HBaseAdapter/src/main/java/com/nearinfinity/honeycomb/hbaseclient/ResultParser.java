package com.nearinfinity.honeycomb.hbaseclient;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.UUID;

import static com.nearinfinity.honeycomb.mysql.Util.UUID_WIDTH;

public class ResultParser {
    private static final Logger logger = Logger.getLogger(ResultParser.class);

    /**
     * Extracts the unique identifier from an HBase {@code Result} that is a data row.
     *
     * @param result HBase data row result
     * @return Unique identifier
     */
    public static UUID parseUUID(Result result) {
        byte[] rowKey = result.getRow();
        if (logger.isDebugEnabled()) {
            logger.debug("UUID parse: " + Bytes.toStringBinary(rowKey));
        }
        ByteBuffer byteBuffer = ByteBuffer.wrap(rowKey, rowKey.length - UUID_WIDTH, UUID_WIDTH);
        return new UUID(byteBuffer.getLong(), byteBuffer.getLong());
    }
}
