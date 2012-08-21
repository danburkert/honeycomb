package com.nearinfinity.mysqlengine.strategy;

import com.nearinfinity.mysqlengine.Constants;
import com.nearinfinity.mysqlengine.RowKeyFactory;
import com.nearinfinity.mysqlengine.ScanFactory;
import com.nearinfinity.mysqlengine.TableInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

/**
 * Created with IntelliJ IDEA.
 * User: jedstrom
 * Date: 8/21/12
 * Time: 8:27 AM
 * To change this template use File | Settings | File Templates.
 */
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
