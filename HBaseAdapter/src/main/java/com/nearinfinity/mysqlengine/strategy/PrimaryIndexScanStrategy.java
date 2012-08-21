package com.nearinfinity.mysqlengine.strategy;

import com.nearinfinity.mysqlengine.*;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;

import java.util.LinkedList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: jedstrom
 * Date: 8/21/12
 * Time: 8:25 AM
 * To change this template use File | Settings | File Templates.
 */
public class PrimaryIndexScanStrategy extends ScanStrategyBase {
    public PrimaryIndexScanStrategy(String tableName, String columnName, byte[] value) {
        super(tableName, columnName, value);
    }

    @Override
    public Scan getScan(TableInfo info) {
        long tableId = info.getId();
        long columnId = info.getColumnIdByName(columnName);

        byte[] startRow = RowKeyFactory.buildValueIndexKey(tableId, columnId, value, Constants.ZERO_UUID);
        byte[] endRow = RowKeyFactory.buildValueIndexKey(tableId, columnId, value, Constants.FULL_UUID);

        Scan scan = ScanFactory.buildScan();
        scan.setStartRow(startRow);

        List<Filter> filterList = new LinkedList<Filter>();
        filterList.add(new InclusiveStopFilter(endRow));
        filterList.add(new ExactValueFilter(value));

        scan.setFilter(new FilterList(filterList));

        return scan;
    }
}
