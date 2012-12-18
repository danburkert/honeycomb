package com.nearinfinity.honeycomb.hbaseclient;

import com.google.common.base.Joiner;
import com.nearinfinity.honeycomb.hbaseclient.strategy.PrefixScanStrategy;
import com.nearinfinity.honeycomb.hbaseclient.strategy.ScanStrategy;
import com.nearinfinity.honeycomb.hbaseclient.strategy.ScanStrategyInfo;
import com.nearinfinity.honeycomb.mysqlengine.HBaseResultScanner;
import com.nearinfinity.honeycomb.mysqlengine.SingleResultScanner;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class HBaseReader {
    private static final Logger logger = Logger.getLogger(HBaseReader.class);

    private final HTableInterface table;

    public HBaseReader(HTableInterface table) {
        this.table = table;
    }

    private <K, V> Map<K, V> selectKeys(Map<K, V> map, Iterable<K> keys) {
        Map<K, V> subset = new HashMap<K, V>();
        for (K key : keys) {
            V value = map.get(key);
            if (map.containsKey(key) && value != null) {
                subset.put(key, value);
            }
        }

        return subset;
    }

    public Result getDataRow(UUID uuid, String tableName) throws IOException {
        TableInfo info = getTableInfo(tableName);
        long tableId = info.getId();

        byte[] rowKey = RowKeyFactory.buildDataKey(tableId, uuid);

        Get get = new Get(rowKey);
        return table.get(get);
    }

    public String findDuplicateKeyOnUpdate(final String tableName, final Map<String, byte[]> values, List<String> changedColumns) throws IOException {
        if (values.isEmpty()) {
            return null;
        }

        Map<String, byte[]> subMap = selectKeys(values, changedColumns);
        return findDuplicateKey(tableName, subMap);
    }

    public String findDuplicateKey(final String tableName, final Map<String, byte[]> values) throws IOException {
        TableInfo info = getTableInfo(tableName);
        List<List<String>> allUniqueKeys = Index.uniqueKeysForTable(info.tableMetadata());
        for (List<String> uniqueKeySet : allUniqueKeys) {
            Map<String, byte[]> subKeyMap = selectKeys(values, uniqueKeySet);
            if (subKeyMap.size() == 0) {
                continue;
            }

            String duplicateFound = doFindDuplicateKey(tableName, subKeyMap);
            if (duplicateFound != null) {
                return duplicateFound;
            }
        }

        return null;
    }

    private String doFindDuplicateKey(String tableName, Map<String, byte[]> values) throws IOException {
        ScanStrategyInfo scanInfo = new ScanStrategyInfo(tableName, values.keySet(), valueMapToKeyValues(tableName, values));
        PrefixScanStrategy strategy = new PrefixScanStrategy(scanInfo);

        HBaseResultScanner scanner = new SingleResultScanner(getScanner(strategy));

        if (scanner.next(null) != null) {
            return Joiner.on(",").join(scanInfo.columnNames());
        }

        return null;
    }

    public byte[] findDuplicateValue(String tableName, String columnNameStrings) throws IOException {
        TableInfo info = getTableInfo(tableName);
        List<String> columnNames = Arrays.asList(columnNameStrings.split(","));

        Scan scan = ScanFactory.buildScan();
        byte[] prefix = ByteBuffer.allocate(9).put(RowType.DATA.getValue()).putLong(info.getId()).array();
        PrefixFilter prefixFilter = new PrefixFilter(prefix);

        List<byte[]> columnIds = new LinkedList<byte[]>();
        for (String columnName : columnNames) {
            byte[] columnIdBytes = Bytes.toBytes(info.getColumnIdByName(columnName));
            columnIds.add(columnIdBytes);
            scan.addColumn(Constants.NIC, columnIdBytes);
        }

        scan.setFilter(prefixFilter);

        Set<ByteBuffer> columnValues = new HashSet<ByteBuffer>();

        ResultScanner scanner = table.getScanner(scan);
        Result result;

        while ((result = scanner.next()) != null) {
            List<byte[]> values = new LinkedList<byte[]>();
            int size = 0;
            for (byte[] columnIdBytes : columnIds) {
                byte[] value = result.getValue(Constants.NIC, columnIdBytes);
                values.add(value);
                size += value.length;
            }
            ByteBuffer value = ByteBuffer.wrap(Util.mergeByteArrays(values, size));

            if (columnValues.contains(value)) {
                return value.array();
            }

            columnValues.add(value);
        }

        return null;
    }

    public void setupKeyValues(String tableName, List<String> columnName, List<KeyValue> keyValues, byte fill) throws IOException {
        TableInfo info = getTableInfo(tableName);
        for (String column : columnName) {
            ColumnMetadata metadata = info.getColumnMetadata(column);
            byte[] value = new byte[metadata.getMaxLength()];
            Arrays.fill(value, fill);
            keyValues.add(new KeyValue(column, value, metadata.isNullable(), false));
        }
    }

    public boolean isNullable(String tableName, String columnName) throws IOException {
        TableInfo info = getTableInfo(tableName);
        return info.getColumnMetadata(columnName).isNullable();
    }

    public long getAutoincrementValue(String tableName, String fieldName) throws IOException {
        TableInfo tableInfo = getTableInfo(tableName);
        ColumnMetadata columnMetadata = tableInfo.getColumnMetadata(fieldName);

        logger.info("isAutoincrement:" + columnMetadata.isAutoincrement() + ", autoincrementValue:" + columnMetadata.getAutoincrementValue());

        if (!columnMetadata.isAutoincrement())
            return -1L;
        else
            return columnMetadata.getAutoincrementValue();
    }

    public long getRowCount(String tableName) throws IOException {
        TableInfo tableInfo = getTableInfo(tableName);
        long tableId = tableInfo.getId();
        byte[] rowKey = RowKeyFactory.buildTableInfoKey(tableId);
        return table.incrementColumnValue(rowKey, Constants.NIC, Constants.ROW_COUNT, 0);
    }

    private List<KeyValue> valueMapToKeyValues(String tableName, Map<String, byte[]> valueMap) throws IOException {
        TableInfo info = getTableInfo(tableName);
        List<KeyValue> keyValues = new LinkedList<KeyValue>();
        for (Map.Entry<String, byte[]> entry : valueMap.entrySet()) {
            String key = entry.getKey();
            ColumnMetadata metadata = info.getColumnMetadata(key);
            byte[] value = entry.getValue();
            keyValues.add(new KeyValue(key, value, metadata.isNullable(), value == null));
        }

        return keyValues;
    }

    public ResultScanner getScanner(ScanStrategy strategy) throws IOException {
        TableInfo info = getTableInfo(strategy.getTableName());

        Scan scan = strategy.getScan(info);

        return table.getScanner(scan);
    }

    public void setCacheSize(int cacheSize) {
        logger.info("Setting table scan row cache to " + cacheSize);
        ScanFactory.setCacheAmount(cacheSize);
    }

    public TableInfo getTableInfo(String tableName) throws IOException {
        return TableCache.getTableInfo(tableName, table);
    }
}
