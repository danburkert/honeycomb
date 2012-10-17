package com.nearinfinity.hbaseclient.strategy;

import com.nearinfinity.hbaseclient.*;
import org.apache.hadoop.hbase.client.Scan;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;

public class ReverseScanStrategy extends ScanStrategyBase {

    public ReverseScanStrategy(String tableName, List<String> columnName, byte[] value) {
        super(tableName, columnName, value);
    }

    @Override
    public Scan getScan(TableInfo info) {
        long tableId = info.getId();
        long columnId = info.getColumnIdByName(columnName);
        ColumnType columnType = info.getColumnTypeByName(columnName);

        byte[] encodedValue = ValueEncoder.descendingEncode(value, columnType, 0);
        byte[] startKey = RowKeyFactory.buildReverseIndexKey(tableId, columnId, encodedValue, Constants.ZERO_UUID);
        byte[] endKey = RowKeyFactory.buildReverseIndexKey(tableId, columnId + 1, new byte[value.length], Constants.ZERO_UUID);

        return ScanFactory.buildScan(startKey, endKey);
    }
}
