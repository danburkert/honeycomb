package com.nearinfinity.hbaseclient.strategy;

import com.nearinfinity.hbaseclient.*;
import org.apache.hadoop.hbase.client.Scan;

import java.nio.ByteBuffer;
import java.util.List;

public class OrderedScanStrategy extends ScanStrategyBase {

    public OrderedScanStrategy(String tableName, List<String> columnName, byte[] value) {
        super(tableName, columnName, value);
    }

    @Override
    public Scan getScan(TableInfo info) {
        long tableId = info.getId();
        long columnId = info.getColumnIdByName(this.columnName);
        ColumnType columnType = info.getColumnTypeByName(this.columnName);

        if (columnType == ColumnType.STRING || columnType == ColumnType.BINARY) {
            int maxLength = info.getColumnMetadata(columnName).getMaxLength();
            value = new byte[maxLength];
        }

        byte[] encodedValue = ValueEncoder.encodeValue(value, columnType);
        byte[] paddedValue = ValueEncoder.padValueAscending(encodedValue, 0);
        byte[] startKey = RowKeyFactory.buildValueIndexKey(tableId, columnId, paddedValue, Constants.ZERO_UUID);
        byte[] endKey = RowKeyFactory.buildValueIndexKey(tableId, columnId + 1, paddedValue, Constants.ZERO_UUID);

        return ScanFactory.buildScan(startKey, endKey);
    }
}
