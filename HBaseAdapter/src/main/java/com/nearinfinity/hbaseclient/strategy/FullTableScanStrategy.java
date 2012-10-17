package com.nearinfinity.hbaseclient.strategy;

import com.nearinfinity.hbaseclient.Constants;
import com.nearinfinity.hbaseclient.RowKeyFactory;
import com.nearinfinity.hbaseclient.ScanFactory;
import com.nearinfinity.hbaseclient.TableInfo;
import org.apache.hadoop.hbase.client.Scan;

public class FullTableScanStrategy extends ScanStrategyBase {

    public FullTableScanStrategy(String tableName, String columnName, byte[] value) {
        super(tableName, columnName, value);
    }

    @Override
    public Scan getScan(TableInfo info) {
        long tableId = info.getId();

        //Build row keys
        byte[] startRow = RowKeyFactory.buildDataKey(tableId, Constants.ZERO_UUID);
        byte[] endRow = RowKeyFactory.buildDataKey(tableId + 1, Constants.ZERO_UUID);

        return ScanFactory.buildScan(startRow, endRow);
    }
}
