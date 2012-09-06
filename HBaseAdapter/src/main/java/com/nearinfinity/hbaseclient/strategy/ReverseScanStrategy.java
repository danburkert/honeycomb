package com.nearinfinity.hbaseclient.strategy;

import com.nearinfinity.hbaseclient.*;
import org.apache.hadoop.hbase.client.Scan;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Created with IntelliJ IDEA.
 * User: jedstrom
 * Date: 8/21/12
 * Time: 8:20 AM
 * To change this template use File | Settings | File Templates.
 */
public class ReverseScanStrategy extends ScanStrategyBase {

    private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(ReverseScanStrategy.class);

    public ReverseScanStrategy(String tableName, String columnName, byte[] value) {
        super(tableName, columnName, value);
    }

    @Override
    public Scan getScan(TableInfo info) {
        long tableId = info.getId();
        long columnId = info.getColumnIdByName(columnName);

        ColumnType columnType = info.getColumnTypeByName(columnName);

        byte[] startKey = RowKeyFactory.buildReverseIndexKey(tableId, columnId, value, columnType, Constants.ZERO_UUID, 0);
        byte[] endKey = RowKeyFactory.buildEmptyValueReverseIndexKey(tableId, columnId+1, value, columnType, Constants.ZERO_UUID, 0);

        return ScanFactory.buildScan(startKey, endKey);
    }
}
