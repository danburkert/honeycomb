package com.nearinfinity.honeycomb.hbaseclient.strategy;

import java.util.Map;

import org.apache.hadoop.hbase.client.Scan;

import com.nearinfinity.honeycomb.hbaseclient.Constants;
import com.nearinfinity.honeycomb.hbaseclient.Index;
import com.nearinfinity.honeycomb.hbaseclient.RowKeyFactory;
import com.nearinfinity.honeycomb.hbaseclient.ScanFactory;
import com.nearinfinity.honeycomb.hbaseclient.TableInfo;
import com.nearinfinity.honeycomb.hbaseclient.ValueEncoder;

public class PrefixScanStrategy implements ScanStrategy {
    private final ScanStrategyInfo scanInfo;

    public PrefixScanStrategy(ScanStrategyInfo scanInfo) {
        this.scanInfo = scanInfo;
    }

    @Override
    public Scan getScan(TableInfo info) {
        long tableId = info.getId();
        Map<String, byte[]> ascendingValueMap = ValueEncoder.correctAscendingValuePadding(info, scanInfo.keyValueMap(), scanInfo.nullSearchColumns());
        byte[] columnId = Index.createColumnIds(scanInfo.columnNames(), info.columnNameToIdMap());
        byte[] paddedValue = Index.convertToHBaseFormat(scanInfo.keyValueColumns(), ascendingValueMap);

        byte[] startKey = RowKeyFactory.buildIndexRowKey(tableId, columnId, paddedValue, Constants.ZERO_UUID);
        byte[] endKey = RowKeyFactory.buildIndexRowKey(tableId, columnId, paddedValue, Constants.FULL_UUID);

        Scan scan = ScanFactory.buildScan(startKey, endKey);

        return scan;
    }

    @Override
    public String getTableName() {
        return scanInfo.tableName();
    }
}
