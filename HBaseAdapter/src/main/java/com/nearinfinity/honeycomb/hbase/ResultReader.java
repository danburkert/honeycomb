package com.nearinfinity.honeycomb.hbase;

import com.nearinfinity.honeycomb.hbaseclient.Constants;
import com.nearinfinity.honeycomb.hbaseclient.TableInfo;
import com.nearinfinity.honeycomb.mysql.Row;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.nearinfinity.honeycomb.mysql.Util.UUID_WIDTH;

/**
 * Read results from HBase table scans and transform results into the appropriate
 * object.
 */
public class ResultReader {
    private static final Logger logger = Logger.getLogger(ResultReader.class);

    /**
     * Read the result of a scan of a Data row, and return a Row object.
     *
     * @param result Result object of Data row scan
     * @param info   TableInfo of scanned (MySQL) table
     * @return
     */
    public static Row readDataRow(Result result, TableInfo info) {
        Map<String, Object> records = new HashMap<String, Object>();
        Map<byte[], byte[]> returnedColumns = result.getNoVersionMap().get(Constants.NIC);

        UUID uuid = parseUUID(result);

        if (returnedColumns.size() == 1 && returnedColumns.containsKey(new byte[0])) {
            if (logger.isDebugEnabled()) {
                logger.debug("Data row: all null");
            }
            // The row of all nulls special case strikes again
            return new Row(records, uuid);
        }

        for (Map.Entry<byte[], byte[]> entry : returnedColumns.entrySet()) {
            long columnId = ByteBuffer.wrap(entry.getKey()).getLong();
            String columnName = info.getColumnNameById(columnId);
            records.put(columnName, ByteBuffer.wrap(entry.getValue()));
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Data row: " + records.toString());
        }

        return new Row(records, uuid);
    }

    /**
     * Read the result of scan of an Index row, and return a Row object.
     *
     * @param result
     * @return Row object
     * @throws IOException Thrown if the result cannot be deserialized into a Row
     */
    public static Row readIndexRow(Result result) throws IOException {
        assert (result != null);
        byte[] serializedRow = result.getValue(Constants.NIC, Constants.VALUE_MAP);
        return Row.deserialize(serializedRow);
    }

    /**
     * Extracts the UUID from an HBase {@code Result} containing a data row.
     *
     * @param result HBase data row result
     * @return UUID bytes
     */
    private static UUID parseUUID(Result result) {
        byte[] rowKey = result.getRow();
        if (logger.isDebugEnabled()) {
            logger.debug("UUID parse: " + Bytes.toStringBinary(rowKey));
        }
        assert (rowKey.length >= UUID_WIDTH);
        ByteBuffer uuidBytes = ByteBuffer.wrap(rowKey, rowKey.length - UUID_WIDTH, UUID_WIDTH);
        return new UUID(uuidBytes.getLong(), uuidBytes.getLong());
    }
}
