package com.nearinfinity.hbaseclient.strategy;

import com.nearinfinity.hbaseclient.Constants;
import com.nearinfinity.hbaseclient.RowKeyFactory;
import com.nearinfinity.hbaseclient.ScanFactory;
import com.nearinfinity.hbaseclient.TableInfo;
import org.apache.hadoop.hbase.client.Scan;

public class NullScanStrategy extends ScanStrategyBase {
    public NullScanStrategy(String tableName, String columnName, byte[] value) {
        super(tableName, columnName, value);
    }

    @Override
    public Scan getScan(TableInfo info) {
        long tableId = info.getId();
        long columnId = info.getColumnIdByName(columnName);

        byte[] startKey = RowKeyFactory.buildNullIndexKey(tableId, columnId, Constants.ZERO_UUID);
        byte[] endKey = RowKeyFactory.buildNullIndexKey(tableId, columnId+1, Constants.ZERO_UUID);

        return ScanFactory.buildScan(startKey, endKey);
    }
}
