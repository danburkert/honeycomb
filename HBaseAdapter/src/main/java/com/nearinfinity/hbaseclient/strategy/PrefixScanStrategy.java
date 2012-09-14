package com.nearinfinity.hbaseclient.strategy;

import com.nearinfinity.hbaseclient.*;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RowFilter;

import java.util.logging.Logger;

/**
 * Created with IntelliJ IDEA.
 * User: jedstrom
 * Date: 8/21/12
 * Time: 8:18 AM
 * To change this template use File | Settings | File Templates.
 */
public class PrefixScanStrategy extends ScanStrategyBase {

    public PrefixScanStrategy(String tableName, String columnName, byte[] value) {
        super(tableName, columnName, value);
    }

    @Override
    public Scan getScan(TableInfo info) {
        long tableId = info.getId();
        long columnId = info.getColumnIdByName(this.columnName);
        ColumnType columnType = info.getColumnTypeByName(this.columnName);

        byte[] startKey = RowKeyFactory.buildValueIndexKey(tableId, columnId, value, Constants.ZERO_UUID, columnType, 0);
        byte[] endKey = RowKeyFactory.buildValueIndexKey(tableId, columnId, value, Constants.FULL_UUID, columnType, 0);

        byte[] prefix = RowKeyFactory.buildValueIndexPrefix(tableId, columnId, value, columnType);

        Scan scan = ScanFactory.buildScan(startKey, endKey);

        PrefixFilter filter = new PrefixFilter(prefix);

        scan.setFilter(filter);

        return scan;
    }
}
