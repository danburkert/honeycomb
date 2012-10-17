package com.nearinfinity.hbaseclient.strategy;

import com.nearinfinity.hbaseclient.*;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RowFilter;

import java.util.List;
import java.util.logging.Logger;

public class PrefixScanStrategy extends ScanStrategyBase {

    public PrefixScanStrategy(String tableName, List<String> columnName, byte[] value) {
        super(tableName, columnName, value);
    }

    @Override
    public Scan getScan(TableInfo info) {
        long tableId = info.getId();
        ColumnType columnType = info.getColumnTypeByName(this.columnName);
        byte[] columnId = Index.createColumnIds(this.columnName, info.columnNameToIdMap());
        byte[] paddedValue = ValueEncoder.ascendingEncode(value, columnType, 0);

        byte[] startKey = RowKeyFactory.buildIndexKey(tableId, columnId, paddedValue, Constants.ZERO_UUID);
        byte[] endKey = RowKeyFactory.buildIndexKey(tableId, columnId, paddedValue, Constants.FULL_UUID);

        byte[] prefix = RowKeyFactory.buildValueIndexPrefix(tableId, columnId, value);

        Scan scan = ScanFactory.buildScan(startKey, endKey);

        PrefixFilter filter = new PrefixFilter(prefix);

        scan.setFilter(filter);

        return scan;
    }
}
