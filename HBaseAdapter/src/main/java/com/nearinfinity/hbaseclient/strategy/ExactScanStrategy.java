package com.nearinfinity.hbaseclient.strategy;

import com.nearinfinity.hbaseclient.ColumnMetadata;
import com.nearinfinity.hbaseclient.RowKeyFactory;
import com.nearinfinity.hbaseclient.ScanFactory;
import com.nearinfinity.hbaseclient.TableInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;

/**
 * Created with IntelliJ IDEA.
 * User: jedstrom
 * Date: 8/21/12
 * Time: 8:18 AM
 * To change this template use File | Settings | File Templates.
 */
public class ExactScanStrategy extends ScanStrategyBase {

    public ExactScanStrategy(String tableName, String columnName, byte[] value) {
        super(tableName, columnName, value);
    }

    @Override
    public Scan getScan(TableInfo info) {
        long tableId = info.getId();
        long columnId = info.getColumnIdByName(this.columnName);
        ColumnMetadata columnType = info.getColumnTypeByName(this.columnName);

        byte[] startKey = RowKeyFactory.buildSecondaryIndexKey(tableId, columnId, value, columnType);
        byte[] endKey = RowKeyFactory.buildSecondaryIndexKey(tableId, columnId+1, new byte[0], ColumnMetadata.NONE);

        Scan scan = ScanFactory.buildScan(startKey, endKey);

        RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(startKey));
        scan.setFilter(filter);

        return scan;
    }
}
