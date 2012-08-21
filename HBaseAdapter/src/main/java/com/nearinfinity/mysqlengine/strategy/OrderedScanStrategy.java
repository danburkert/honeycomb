package com.nearinfinity.mysqlengine.strategy;

import com.nearinfinity.mysqlengine.*;
import org.apache.hadoop.hbase.client.Scan;

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
        ColumnMetadata columnType = info.getColumnTypeByName(this.columnName);

        byte[] startKey = RowKeyFactory.buildSecondaryIndexKey(tableId, columnId, value, columnType);
        byte[] endKey = RowKeyFactory.buildSecondaryIndexKey(tableId, columnId+1, new byte[0], ColumnMetadata.NONE);

        return ScanFactory.buildScan(startKey, endKey);
    }
}
