package com.nearinfinity.honeycomb.hbaseclient;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

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
        ByteBuffer byteBuffer = ByteBuffer.wrap(rowKey, rowKey.length - 16, 16);
        return new UUID(byteBuffer.getLong(), byteBuffer.getLong());
    }

    /**
     * Extracts the raw SQL row from an HBase {@code Result} that is an index row.
     *
     * @param result HBase index row result
     * @return SQL row
     */
    public static byte[] parseValueMap(Result result) {
        byte[] valueMap = result.getValue(Constants.NIC, Constants.VALUE_MAP);
        if (logger.isDebugEnabled()) {
            logger.debug("Index row: " + new String(valueMap));
        }

        return valueMap;
    }

    /**
     * Extracts the SQL row from an HBase {@code Result} that is an index row.
     *
     * @param result HBase index row result
     * @return SQL row
     */
    public static Map<String, byte[]> parseRowMap(Result result) {
        byte[] mapBytes = parseValueMap(result);
        return Util.deserializeMap(mapBytes);
    }

    /**
     * Extracts the SQL row from an HBase {@code Result} that is a data row.
     *
     * @param result HBase data row result
     * @param info   Table metadata
     * @return SQL row
     */
    public static Map<String, byte[]> parseDataRow(Result result, TableInfo info) {
        Map<String, byte[]> columns = new HashMap<String, byte[]>();
        Map<byte[], byte[]> returnedColumns = result.getNoVersionMap().get(Constants.NIC);

        if (returnedColumns.size() == 1 && returnedColumns.containsKey(new byte[0])) {
            if (logger.isDebugEnabled()) {
                logger.debug("Data row: all null");
            }
            // The row of all nulls special case strikes again
            return columns;
        }

        for (Map.Entry<byte[], byte[]> entry : returnedColumns.entrySet()) {
            long columnId = ByteBuffer.wrap(entry.getKey()).getLong();
            String columnName = info.getColumnNameById(columnId);
            columns.put(columnName, entry.getValue());
        }

        if (logger.isDebugEnabled()) {
            byte[] map = Util.serializeMap(columns);
            logger.debug("Data row: " + new String(map));
        }

        return columns;
    }
}
