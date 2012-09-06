package com.nearinfinity.hbaseclient.strategy;

import com.nearinfinity.hbaseclient.*;
import org.apache.hadoop.hbase.client.Scan;

import java.nio.ByteBuffer;

/**
 * Created with IntelliJ IDEA.
 * User: jedstrom
 * Date: 8/21/12
 * Time: 7:51 AM
 * To change this template use File | Settings | File Templates.
 */
public class OrderedScanStrategy extends ScanStrategyBase {

    public OrderedScanStrategy(String tableName, String columnName, byte[] value) {
        super(tableName, columnName, value);
    }

    @Override
    public Scan getScan(TableInfo info) {
        long tableId = info.getId();
        long columnId = info.getColumnIdByName(this.columnName);
        ColumnType columnType = info.getColumnTypeByName(this.columnName);

        if (columnType == ColumnType.STRING || columnType == ColumnType.BINARY) {
            byte[] maxLengthArray = info.getColumnMetadata(columnName, ColumnMetadata.MAX_LENGTH);
            int maxLength = (int) ByteBuffer.wrap(maxLengthArray).getLong();
            value = new byte[maxLength];
        }

        byte[] startKey = RowKeyFactory.buildValueIndexKey(tableId, columnId, value, Constants.ZERO_UUID, columnType, 0);
        byte[] endKey = RowKeyFactory.buildValueIndexKey(tableId, columnId + 1, value, Constants.ZERO_UUID, columnType, 0);

        return ScanFactory.buildScan(startKey, endKey);
    }
}
