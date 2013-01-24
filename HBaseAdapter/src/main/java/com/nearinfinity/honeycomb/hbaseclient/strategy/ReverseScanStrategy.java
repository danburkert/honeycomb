package com.nearinfinity.honeycomb.hbaseclient.strategy;

import com.google.common.collect.Iterables;
import com.nearinfinity.honeycomb.hbaseclient.*;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;

public class ReverseScanStrategy implements ScanStrategy {
    private ScanStrategyInfo scanInfo;
    private final boolean indexLast;

    public ReverseScanStrategy(ScanStrategyInfo scanInfo) {
        this(scanInfo, false);
    }

    public ReverseScanStrategy(ScanStrategyInfo scanInfo, boolean indexLast) {
        this.scanInfo = scanInfo;
        this.indexLast = indexLast;
    }

    @Override
    public Scan getScan(TableInfo info) {
        final long tableId = info.getId();
        final Iterable<String> columns = this.scanInfo.columnNames();
        final int columnCount = Iterables.size(columns);
        final int indexValuesFullLength = Index.calculateIndexValuesFullLength(columns, info.columnLengthMap());
        final Map<String, byte[]> descendingValueMap = ValueEncoder.correctDescendingValuePadding(info, this.scanInfo.keyValueMap());

        final byte[] columnIds = Index.createColumnIds(columns, info.columnNameToIdMap());
        final byte[] nextColumnIds = Util.incrementColumn(columnIds, Bytes.SIZEOF_LONG * (columnCount - 1));

        byte[] paddedValue = Index.convertToHBaseFormat(this.scanInfo.keyValueColumns(), descendingValueMap);
        paddedValue = Bytes.padTail(paddedValue, Math.max(indexValuesFullLength - paddedValue.length, 0));

        if (indexLast) {
            paddedValue = new byte[paddedValue.length];
        }

        byte[] startKey = RowKeyFactory.buildReverseIndexRowKey(tableId, columnIds, paddedValue, Constants.ZERO_UUID);
        byte[] endKey = RowKeyFactory.buildReverseIndexRowKey(tableId, nextColumnIds, new byte[paddedValue.length], Constants.ZERO_UUID);

        return ScanFactory.buildScan(startKey, endKey);
    }

    @Override
    public String getTableName() {
        return this.scanInfo.tableName();
    }
}
