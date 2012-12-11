package com.nearinfinity.honeycomb.hbaseclient.strategy;

import com.nearinfinity.honeycomb.hbaseclient.Constants;
import com.nearinfinity.honeycomb.hbaseclient.RowKeyFactory;
import com.nearinfinity.honeycomb.hbaseclient.ScanFactory;
import com.nearinfinity.honeycomb.hbaseclient.TableInfo;
import org.apache.hadoop.hbase.client.Scan;

public class FullTableScanStrategy implements ScanStrategy {
    private final String tableName;

    public FullTableScanStrategy(String tableName) {

        this.tableName = tableName;
    }

    @Override
    public Scan getScan(TableInfo info) {
        long tableId = info.getId();

        byte[] startRow = RowKeyFactory.buildDataKey(tableId, Constants.ZERO_UUID);
        byte[] endRow = RowKeyFactory.buildDataKey(tableId + 1, Constants.ZERO_UUID);

        return ScanFactory.buildScan(startRow, endRow);
    }

    @Override
    public String getTableName() {
        return this.tableName;
    }
}
