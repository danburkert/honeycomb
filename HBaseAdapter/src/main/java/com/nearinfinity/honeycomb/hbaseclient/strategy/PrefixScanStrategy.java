package com.nearinfinity.honeycomb.hbaseclient.strategy;

import com.nearinfinity.honeycomb.hbaseclient.*;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;

import java.util.Map;

public class PrefixScanStrategy implements ScanStrategy {
    private final ScanStrategyInfo scanInfo;

    public PrefixScanStrategy(ScanStrategyInfo scanInfo) {
        this.scanInfo = scanInfo;
    }
    @Override
    public Scan getScan(TableInfo info) {
        long tableId = info.getId();
        Map<String, byte[]> ascendingValueMap = ValueEncoder.correctAscendingValuePadding(info, this.scanInfo.keyValueMap(), this.scanInfo.nullSearchColumns());
        byte[] columnId = Index.createColumnIds(this.scanInfo.columnNames(), info.columnNameToIdMap());
        byte[] paddedValue = Index.createValues(this.scanInfo.keyValueColumns(), ascendingValueMap);

        byte[] startKey = RowKeyFactory.buildIndexRowKey(tableId, columnId, paddedValue, Constants.ZERO_UUID);
        byte[] endKey = RowKeyFactory.buildIndexRowKey(tableId, columnId, paddedValue, Constants.FULL_UUID);

        byte[] prefix = RowKeyFactory.buildValueIndexPrefix(tableId, columnId, paddedValue);

        Scan scan = ScanFactory.buildScan(startKey, endKey);

        PrefixFilter filter = new PrefixFilter(prefix);

        scan.setFilter(filter);

        return scan;
    }

    @Override
    public String getTableName() {
        return this.scanInfo.tableName();
    }
}
