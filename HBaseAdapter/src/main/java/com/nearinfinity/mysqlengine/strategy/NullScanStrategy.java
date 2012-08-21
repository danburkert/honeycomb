package com.nearinfinity.mysqlengine.strategy;

import com.nearinfinity.mysqlengine.Constants;
import com.nearinfinity.mysqlengine.RowKeyFactory;
import com.nearinfinity.mysqlengine.ScanFactory;
import com.nearinfinity.mysqlengine.TableInfo;
import org.apache.hadoop.hbase.client.Scan;

/**
 * Created with IntelliJ IDEA.
 * User: jedstrom
 * Date: 8/21/12
 * Time: 8:47 AM
 * To change this template use File | Settings | File Templates.
 */
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
